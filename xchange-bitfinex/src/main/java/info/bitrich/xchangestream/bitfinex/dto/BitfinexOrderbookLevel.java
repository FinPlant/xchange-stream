package info.bitrich.xchangestream.bitfinex.dto;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import org.knowm.xchange.bitfinex.v1.dto.marketdata.BitfinexLevel;

import java.math.BigDecimal;

/**
 * Created by Lukas Zaoralek on 8.11.17.
 */

public class BitfinexOrderbookLevel {
    private final BigDecimal price;
    private final BigDecimal count;
    private final BigDecimal amount;

    public BitfinexOrderbookLevel(BigDecimal price, BigDecimal count, BigDecimal amount) {
        this.price = price;
        this.amount = amount;
        this.count = count;
    }

    public BitfinexLevel toBitfinexLevel() {
        // Xchange-bitfinex adapter expects the timestamp to be seconds since Epoch.
        return new BitfinexLevel(price, amount, new BigDecimal(System.currentTimeMillis() / 1000));
    }

    public BigDecimal getPrice() {
        return price;
    }

    public BigDecimal getAmount() {
        return amount;
    }

    public BigDecimal getCount() {
        return count;
    }
}
