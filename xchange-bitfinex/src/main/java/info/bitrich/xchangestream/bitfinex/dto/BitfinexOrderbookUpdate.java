package info.bitrich.xchangestream.bitfinex.dto;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.Arrays;

import static info.bitrich.xchangestream.bitfinex.dto.BitfinexOrderbookUpdate.BitfinexOrderBookUpdateDeserializer;

@JsonDeserialize(using = BitfinexOrderBookUpdateDeserializer.class)
public class BitfinexOrderbookUpdate {
    private final String channelId;
    private final BitfinexOrderbookLevel[] levels;

    public BitfinexOrderbookUpdate(String channelId, BitfinexOrderbookLevel[] levels) {
        this.channelId = channelId;
        this.levels = levels;
    }

    public String getChannelId() {
        return channelId;
    }

    public BitfinexOrderbookLevel[] getLevels() {
        return levels;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("BitfinexOrderbookUpdate{");
        sb.append("channelId='").append(channelId).append('\'');
        sb.append(", levels=").append(Arrays.toString(levels));
        sb.append('}');
        return sb.toString();
    }

    protected static class BitfinexOrderBookUpdateDeserializer extends JsonDeserializer<BitfinexOrderbookUpdate> {
        @Override
        public BitfinexOrderbookUpdate deserialize(JsonParser jsonParser, DeserializationContext unused)
                throws IOException, JsonProcessingException {
            final ObjectCodec oc = jsonParser.getCodec();
            final JsonNode node = oc.readTree(jsonParser);

            final String channelID = getChannelId(node);

            final BitfinexOrderbookLevel[] levels = getOrderBookLevels(node);

            return new BitfinexOrderbookUpdate(channelID, levels);
        }

        private BitfinexOrderbookLevel[] getOrderBookLevels(JsonNode node) {
            final int LEVELS = 1;
            final int FIRST_NODE = 0;

            JsonNode levelsNode = node.get(LEVELS);

            if (!levelsNode.get(FIRST_NODE).isArray()) {
                BitfinexOrderbookLevel level = createLevelFromLevelNode(levelsNode);
                return new BitfinexOrderbookLevel[]{level};
            }

            final int levelsCount = levelsNode.size();

            final BitfinexOrderbookLevel[] levels = new BitfinexOrderbookLevel[levelsCount];

            for (int i = 0; i < levelsCount; i++) {
                levels[i] = createLevelFromLevelNode(levelsNode.get(i));
            }

            return levels;
        }

        private BitfinexOrderbookLevel createLevelFromLevelNode(JsonNode levelsNode) {
            final int PRICE = 0;
            final int COUNT = 1;
            final int AMOUNT = 2;

            BigDecimal price = levelsNode.get(PRICE).decimalValue();
            BigDecimal count = levelsNode.get(COUNT).decimalValue();
            BigDecimal amount = levelsNode.get(AMOUNT).decimalValue();

            return new BitfinexOrderbookLevel(price, count, amount);
        }

        private String getChannelId(JsonNode jsonNode) {
            final int CHANNEL_ID = 0;

            return jsonNode.get(CHANNEL_ID).asText();
        }
    }
}
