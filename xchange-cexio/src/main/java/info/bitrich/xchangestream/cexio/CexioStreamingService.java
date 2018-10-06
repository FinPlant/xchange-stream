package info.bitrich.xchangestream.cexio;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import info.bitrich.xchangestream.cexio.dto.*;
import info.bitrich.xchangestream.core.StreamingExchange;
import info.bitrich.xchangestream.service.netty.JsonNettyStreamingService;
import io.reactivex.Observable;
import io.reactivex.subjects.BehaviorSubject;
import org.knowm.xchange.ExchangeSpecification;
import org.knowm.xchange.exceptions.ExchangeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class CexioStreamingService extends JsonNettyStreamingService {

    private static final Logger LOG = LoggerFactory.getLogger(CexioStreamingService.class);

    public static final String CONNECTED = "connected";
    public static final String AUTH = "auth";
    public static final String PING = "ping";
    public static final String PONG = "pong";
    public static final String ORDER = "order";
    public static final String TRANSACTION = "tx";
    public static final String SUBSCRIBE = "subscribe";
    public static final String UNSUBSCRIBE = "unsubscribe";
    public static final String MARKET_DEPTH = "md";

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final StreamingExchange streamingExchange;

    private BehaviorSubject<Boolean> subjectAuthorized = BehaviorSubject.createDefault(false);

    public CexioStreamingService(StreamingExchange streamingExchange, String apiUrl) {
        super(apiUrl, Integer.MAX_VALUE);
        this.streamingExchange = streamingExchange;
    }

    public Observable<Boolean> ready() {
        return Observable
                .combineLatest(subjectAuthorized, connected(), Boolean::logicalAnd)
                .distinctUntilChanged()
                .share();
    }

    @Override
    public String getSubscribeMessage(String channelName, Object... args) throws IOException {

        if (!channelName.equals(CexioStreamingService.MARKET_DEPTH)) {
            return null;
        }

        if (args.length != 1) {
            throw new IllegalArgumentException("Wrong arguments count");
        }
        String room = (String) args[0];

        CexioSubscriptionMessage message = new CexioSubscriptionMessage(room);
        return objectMapper.writeValueAsString(message);
    }

    @Override
    public String getUnsubscribeMessage(String channelName, Object... args) throws IOException {
        if (!channelName.equals(CexioStreamingService.MARKET_DEPTH)) {
            return null;
        }

        if (args.length != 1) {
            throw new IllegalArgumentException("Wrong arguments count");
        }
        String room = (String) args[0];

        CexioUnsubscribeMessage message = new CexioUnsubscribeMessage(room);
        return objectMapper.writeValueAsString(message);
    }

    @Override
    public void messageHandler(String message) {
        super.messageHandler(message);
    }

    @Override
    protected void handleMessage(JsonNode message) {
        JsonNode cexioMessage = message.get("e");
        if (cexioMessage == null) {
            return;
        }

        try {
            switch (cexioMessage.textValue()) {
                case CONNECTED:
                    if (!auth()) {
                        subjectAuthorized.onNext(true);
                    }
                    break;
                case AUTH:
                    CexioAuthResponse response = deserialize(message, CexioAuthResponse.class);
                    if (response != null && !response.isSuccess()) {
                        String error = String.format("Authentication error: %s", response.getData().getError());
                        LOG.error(error);
                        subjectAuthorized.onNext(false);
                        throw new ExchangeException(error);
                    }
                    subjectAuthorized.onNext(true);
                    break;
                case PING:
                    pong();
                    break;
                default:
                    super.handleMessage(message);
            }
        } catch (JsonProcessingException e) {
            LOG.error("Json parsing error: {}", e.getMessage());
        }
    }

    @Override
    protected String getChannelNameFromMessage(JsonNode message) throws IOException {
        JsonNode command = message.get("e");
        if (command == null) {
            return null;
        }

        /* As cex.io don`t provide option, to distinguish order book subscription channels
        * We create unique channel name for order book subscription, from channel name and subscribed
        * currency pair */

        String commandText = command.asText();
        if (!commandText.equals(MARKET_DEPTH)) {
            return commandText;
        }

        JsonNode data = message.get("data");

        if (data == null) {
            LOG.warn("Received market depth with empty data");
            return null;
        }
        JsonNode pair = data.get("pair");

        if (pair == null) {
            LOG.warn("Received market depth with empty pair");
            return null;
        }
        return new StringBuilder(commandText).append("-").append(pair.asText()).toString();
    }

    @Override
    public String getSubscriptionUniqueId(String channelName, Object... args) {
        if (!channelName.equals(MARKET_DEPTH)) {
            return channelName;
        }

        /* As cex.io don`t provide option, to distinguish order book subscription channels.
         * We create channel unique name for order book subscription, from channel name and subscribed
         * currency pair. */

        if (args.length < 1) {
            throw new IllegalArgumentException("Unexpected args for order book subscription");
        }

        String[] roomData = args[0].toString().split("-");

        if (roomData.length != 3) {
            throw new IllegalArgumentException("Unexpected args for order book subscription");
        } else {
            final int BASE_CURRENCY = 1;
            final int COUNTER_CURRENCY = 2;

            return new StringBuilder(channelName)
                    .append("-")
                    .append(roomData[BASE_CURRENCY])
                    .append(":")
                    .append(roomData[COUNTER_CURRENCY])
                    .toString();
        }

    }

    private boolean auth() {
        ExchangeSpecification specification = streamingExchange.getExchangeSpecification();
        String secretKey = specification.getSecretKey();
        String apiKey = specification.getApiKey();

        if (apiKey == null || secretKey == null) {
            LOG.debug("Credentials are not defined. Skip authorisation");
            return false;
        }

        CexioDigest cexioDigest = CexioDigest.createInstance(secretKey);

        long timestamp = System.currentTimeMillis() / 1000;
        String signature = cexioDigest.createSignature(timestamp, apiKey);
        CexioAuthMessage message =
                new CexioAuthMessage(new CexioAuthRequest(apiKey, signature, timestamp));
        sendMessage(message);

        return true;
    }

    private void pong() {
        CexioPongMessage message = new CexioPongMessage();
        sendMessage(message);
    }

    private void sendMessage(Object message) {
        try {
            sendMessage(objectMapper.writeValueAsString(message));
        } catch (JsonProcessingException e) {
            LOG.error("Error creating json message: {}", e.getMessage());
        }
    }

    private <T> T deserialize(JsonNode message, Class<T> clazz) throws JsonProcessingException {
        return objectMapper.treeToValue(message, clazz);
    }
}
