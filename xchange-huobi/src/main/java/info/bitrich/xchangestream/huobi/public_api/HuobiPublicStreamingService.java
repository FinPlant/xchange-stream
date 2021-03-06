package info.bitrich.xchangestream.huobi.public_api;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import info.bitrich.xchangestream.core.StreamingExchange;
import info.bitrich.xchangestream.huobi.netty.HuobiHeartbeatStrategy;
import info.bitrich.xchangestream.huobi.public_api.dto.HuobiPongMessage;
import info.bitrich.xchangestream.huobi.public_api.dto.HuobiSubscribeRequest;
import info.bitrich.xchangestream.huobi.public_api.dto.HuobiUnsubscribeRequest;
import info.bitrich.xchangestream.service.netty.JsonNettyStreamingService;
import info.bitrich.xchangestream.service.netty.NettyStreamingService;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.reactivex.Observable;
import org.knowm.xchange.exceptions.ExchangeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.zip.GZIPInputStream;

public class HuobiPublicStreamingService extends JsonNettyStreamingService {

    private static final Logger LOG = LoggerFactory.getLogger(HuobiPublicStreamingService.class);

    private final ObjectMapper mapper = new ObjectMapper();
    private final Map<String, String> subscriptionRequests = new ConcurrentHashMap<>();

    public HuobiPublicStreamingService(StreamingExchange exchange, String apiUrl) {
        super(apiUrl, Integer.MAX_VALUE,
                NettyStreamingService.DEFAULT_CONNECTION_TIMEOUT, NettyStreamingService.DEFAULT_RETRY_DURATION,
                new HuobiHeartbeatStrategy());
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    @Override
    protected String getChannelNameFromMessage(JsonNode message) {
        return message.get("ch").asText();
    }

    @Override
    public String getSubscribeMessage(String channelName, Object... args) throws IOException {
        HuobiSubscribeRequest message = new HuobiSubscribeRequest(channelName);
        subscriptionRequests.put(message.getId(), channelName);
        return mapper.writeValueAsString(message);
    }

    @Override
    public String getUnsubscribeMessage(String channelName, Object... args) throws IOException {
        HuobiUnsubscribeRequest message = new HuobiUnsubscribeRequest(channelName);
        return mapper.writeValueAsString(message);
    }

    public Observable<Boolean> ready() {
        return connected();
    }

    @Override
    protected void messageHandler(ByteBuf message) {

        StringBuilder output = new StringBuilder();

        ByteBufInputStream stream = new ByteBufInputStream(message);
        try (InputStream gzipInputStream = new GZIPInputStream(stream)) {
            InputStreamReader streamReader = new InputStreamReader(gzipInputStream, StandardCharsets.UTF_8);
            try (BufferedReader bufferedReader = new BufferedReader(streamReader)) {

                String line;
                while ((line = bufferedReader.readLine()) != null) {
                    output.append(line);
                }

                super.messageHandler(output.toString());
            }
        } catch (IOException e) {
            LOG.error("Encoded message unpack error: %s", e.getMessage(), e);
        }
    }

    @Override
    protected void handleMessage(JsonNode message) {

        LOG.trace("=>: {}", message);

        if (handleErrorIfExists(message)) {
            return;
        }

        if (handlePingIfExists(message)) {
            return;
        }

        if (handlePongIfExists(message)) {
            return;
        }

        JsonNode subbedNode = message.get("subbed");
        if (subbedNode != null) {
            String channel = subbedNode.asText();
            String id = message.get("id").asText();
            subscriptionRequests.remove(id);
            LOG.info("Subscription to '{}' is successful", channel);
            return;
        }

        JsonNode unsubbedNode = message.get("unsubbed");
        if (unsubbedNode != null) {
            String channel = unsubbedNode.asText();
            LOG.info("Unsubscribe from '{}' is successful", channel);
            return;
        }

        super.handleMessage(message);
    }

    private boolean handleErrorIfExists(JsonNode message) {

        JsonNode status = message.get("status");
        if (status == null || status.asText().equals("ok")) {
            return false;
        }

        LOG.error("Exchange returns an error: {}", message);

        String errCode = message.get("err-code").asText();
        String errMessage = message.get("err-msg").asText();

        JsonNode idNode = message.get("id");
        if (idNode == null) {
            LOG.warn("Error message has no request id");
            return true;
        }

        String id = idNode.asText();
        String channel = subscriptionRequests.remove(id);
        if (channel == null) {
            LOG.error("Got error from exchange for unknown request {}: {}", id, errMessage);
            return true;
        }

        handleChannelError(channel, new ExchangeException(String.format("%s: %s", errCode, errMessage)));
        return true;
    }

    private boolean handlePingIfExists(JsonNode message) {

        JsonNode ping = message.get("ping");
        if (ping != null) {
            try {
                sendMessage(mapper.writeValueAsString(new HuobiPongMessage(ping.asLong())));
            } catch (JsonProcessingException e) {
                LOG.error("Fail to serialize pong message: {}", e.getMessage(), e);
            }
            return true;
        }

        return false;
    }

    private boolean handlePongIfExists(JsonNode message) {

        JsonNode pong = message.get("pong");
        if (pong != null) {
            long pongTime = pong.asLong();
            LOG.debug("Ping responded at {}ms", System.currentTimeMillis() - pongTime);
            return true;
        }

        return false;
    }
}
