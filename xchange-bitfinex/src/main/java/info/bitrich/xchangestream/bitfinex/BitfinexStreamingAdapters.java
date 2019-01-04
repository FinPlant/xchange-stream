package info.bitrich.xchangestream.bitfinex;

import info.bitrich.xchangestream.bitfinex.dto.BitfinexOrderbookLevel;
import info.bitrich.xchangestream.bitfinex.dto.BitfinexOrderbookUpdate;
import info.bitrich.xchangestream.bitfinex.dto.BitfinexWebSocketOrder;
import org.knowm.xchange.currency.CurrencyPair;
import org.knowm.xchange.dto.Order;
import org.knowm.xchange.dto.marketdata.OrderBookUpdate;
import org.knowm.xchange.dto.trade.LimitOrder;
import org.knowm.xchange.dto.trade.MarketOrder;
import org.knowm.xchange.exceptions.NotYetImplementedForExchangeException;
import org.knowm.xchange.utils.DateUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.math.BigDecimal.ZERO;

public class BitfinexStreamingAdapters {

    private static final Logger log = LoggerFactory.getLogger(BitfinexStreamingAdapters.class);

    public static List<OrderBookUpdate> adaptOrderBookUpdates(BitfinexOrderbookUpdate bitfinexOrderbookUpdate,
                                                              CurrencyPair currencyPair) {
        return Stream.of(bitfinexOrderbookUpdate.getLevels())
                .map(level -> BitfinexStreamingAdapters.levelToOrderBookUpdate(level, currencyPair))
                .collect(Collectors.toList());
    }

    public static Order adaptOrder(BitfinexWebSocketOrder order) {
        switch (order.getType()) {
            case "LIMIT":
            case "EXCHANGE LIMIT":
                return adaptLimitOrder(order);
            case "MARKET":
            case "EXCHANGE MARKET":
                return adaptMarketOrder(order);
            default:
                throw new NotYetImplementedForExchangeException(
                        String.format("Type %s doesn't implemented", order.getType()));
        }
    }

    private static OrderBookUpdate levelToOrderBookUpdate(BitfinexOrderbookLevel level, CurrencyPair currencyPair) {
        Order.OrderType orderType;
        if (level.getAmount().compareTo(ZERO) < 0) {
            orderType = Order.OrderType.ASK;
        } else {
            orderType = Order.OrderType.BID;
        }

        return new OrderBookUpdate(orderType, level.getAmount(), currencyPair, level.getPrice(), null, null);
    }

    private static Order adaptMarketOrder(BitfinexWebSocketOrder order) {
        try {
            MarketOrder.Builder builder = (MarketOrder.Builder) new MarketOrder.Builder(
                    adaptType(order.getAmountOrig()), adaptSymbol(order.getSymbol()))
                    .id(String.valueOf(order.getId()))
                    .averagePrice(order.getPriceAvg())
                    .orderStatus(adaptStatus(order))
                    .originalAmount(adaptAmount(order.getAmountOrig()))
                    .remainingAmount(adaptAmount(order.getAmount()))
                    .timestamp(DateUtils.fromMillisUtc(order.getMtsCreate()));
            return builder.build();
        } catch (Exception e) {
            throw new IllegalArgumentException(String.format("Unexpected data in an order: %s", order), e);
        }
    }

    private static Order adaptLimitOrder(BitfinexWebSocketOrder order) {
        try {
            return new LimitOrder.Builder(adaptType(order.getAmountOrig()), adaptSymbol(order.getSymbol()))
                    .id(String.valueOf(order.getId()))
                    .limitPrice(order.getPrice())
                    .averagePrice(order.getPriceAvg())
                    .orderStatus(adaptStatus(order))
                    .originalAmount(adaptAmount(order.getAmountOrig()))
                    .remainingAmount(adaptAmount(order.getAmount()))
                    .timestamp(DateUtils.fromMillisUtc(order.getMtsCreate()))
                    .build();
        } catch (Exception e) {
            throw new IllegalArgumentException(String.format("Unexpected data in an order: %s", order), e);
        }
    }

    private static Order.OrderStatus adaptStatus(BitfinexWebSocketOrder order) {

        String status = order.getOrderStatus();

        if (status == null) {
            log.warn("Missing status: {}", status);
            return Order.OrderStatus.UNKNOWN;
        }

        if (status.startsWith("ACTIVE")) {
            return Order.OrderStatus.NEW;
        }
        if (status.startsWith("CANCELED")) {
            return Order.OrderStatus.CANCELED;
        }
        if (status.startsWith("EXECUTED")) {
            return Order.OrderStatus.FILLED;
        }
        if (status.startsWith("PARTIALLY FILLED")) {
            return Order.OrderStatus.PARTIALLY_FILLED;
        }

        log.error("Unknown status in order: {}", order);
        return Order.OrderStatus.UNKNOWN;
    }

    private static CurrencyPair adaptSymbol(String symbol) {
        if (symbol.length() != 7) {
            throw new IllegalArgumentException(String.format("Symbol %s has unexpected length", symbol));
        }

        return new CurrencyPair(symbol.substring(1, 4), symbol.substring(4));
    }

    private static Order.OrderType adaptType(BigDecimal amount) {
        if (amount.signum() == 1) {
            return Order.OrderType.BID;
        }
        if (amount.signum() == -1) {
            return Order.OrderType.ASK;
        }
        throw new IllegalStateException("Amount is zero");
    }

    private static BigDecimal adaptAmount(BigDecimal amount) {
        return amount.abs();
    }
}
