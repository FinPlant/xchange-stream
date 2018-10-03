package info.bitrich.xchangestream.cexio;

import info.bitrich.xchangestream.cexio.dto.CexioOrder;
import info.bitrich.xchangestream.cexio.dto.CexioOrderBookMessage;
import info.bitrich.xchangestream.cexio.dto.CexioCurrencyPair;
import org.knowm.xchange.currency.Currency;
import org.knowm.xchange.currency.CurrencyPair;
import org.knowm.xchange.dto.Order;
import org.knowm.xchange.dto.marketdata.OrderBook;
import org.knowm.xchange.dto.marketdata.Ticker;
import org.knowm.xchange.dto.trade.LimitOrder;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class CexioAdapters {

    private static final int PRECISION = 8;
    private static final BigDecimal SATOSHI_SCALE = new BigDecimal("100000000");

    static Order adaptOrder(CexioOrder order) {
        if (order.getType() != null) {
            return new info.bitrich.xchangestream.cexio.CexioOrder(adaptOrderType(order.getType()),
                    adaptCurrencyPair(order.getPair()),
                    adaptAmount(order.getRemains()),
                    order.getId(),
                    order.getTime(),
                    order.getPrice(),
                    order.getFee(),
                    getOrderStatus(order));
        } else {
            return new info.bitrich.xchangestream.cexio.CexioOrder(adaptCurrencyPair(order.getPair()),
                    order.getId(),
                    getOrderStatus(order),
                    adaptAmount(order.getRemains()));
        }
    }

    static OrderBook adaptOrderBook(CexioOrderBookMessage message) {

        CurrencyPair currencyPair = adaptCurrencyPair(message.getData().getPair());

        List<LimitOrder> buyOrders = message.getData().getBuy().stream()
                .map(it -> adaptOrderBookOrder(Order.OrderType.BID, currencyPair, it))
                .collect(Collectors.toList());
        List<LimitOrder> sellOrders = message.getData().getSell().stream()
                .map(it -> adaptOrderBookOrder(Order.OrderType.ASK, currencyPair, it))
                .collect(Collectors.toList());

        return new OrderBook(new Date(), sellOrders, buyOrders);
    }

    static Ticker adaptOrderBookToTicker(CexioOrderBookMessage message) {

        CurrencyPair currencyPair = adaptCurrencyPair(message.getData().getPair());

        CexioOrderBookMessage.CexioOrderBookData.PriceAmountPair buyPair = message.getData().getBuy().get(0);
        CexioOrderBookMessage.CexioOrderBookData.PriceAmountPair sellPair = message.getData().getSell().get(0);

        return new Ticker.Builder()
                .timestamp(new Date())
                .currencyPair(currencyPair)
                .bid(buyPair.getPrice())
                .bidSize(buyPair.getAmount())
                .ask(sellPair.getPrice())
                .askSize(sellPair.getAmount())
                .build();
    }

    private static LimitOrder adaptOrderBookOrder(Order.OrderType orderType,
                                                  CurrencyPair currencyPair,
                                                  CexioOrderBookMessage.CexioOrderBookData.PriceAmountPair pair) {
        return new LimitOrder.Builder(orderType, currencyPair)
                .limitPrice(pair.getPrice())
                .originalAmount(pair.getAmount())
                .build();
    }

    private static CurrencyPair adaptCurrencyPair(CexioCurrencyPair pair) {
        return new CurrencyPair(pair.getSymbol1(), pair.getSymbol2());
    }

    private static CurrencyPair adaptCurrencyPair(String pair) {
        int split = pair.indexOf(":");
        if (split < 1) {
            throw new IllegalArgumentException("Could not parse currency pair from '" + pair + "'");
        }

        String base = pair.substring(0, split);
        String counter = pair.substring(split + 1);
        return new CurrencyPair(Currency.getInstance(base), Currency.getInstance(counter));
    }

    private static Order.OrderType adaptOrderType(String type) {
        if (type == null) {
            return null;
        }

        switch (type) {
            case "buy":
                return Order.OrderType.BID;
            case "sell":
                return Order.OrderType.ASK;
            default:
                return null;
        }
    }

    private static BigDecimal adaptAmount(BigDecimal amount) {
        if (amount == null) {
            return null;
        }

        return amount.divide(SATOSHI_SCALE, PRECISION, RoundingMode.DOWN);
    }

    private static Order.OrderStatus getOrderStatus(CexioOrder order) {

        BigDecimal remains = order.getRemains();
        BigDecimal amount = order.getAmount();
        Objects.requireNonNull(remains, "Mandatory field 'remains' is absent");

        Order.OrderStatus status;
        if (order.isCancel()) {
            status = Order.OrderStatus.CANCELED;
        } else if (remains.compareTo(BigDecimal.ZERO) == 0) {
            status = Order.OrderStatus.FILLED;
        } else if (amount == null && remains.compareTo(BigDecimal.ZERO) != 0) {
            status = Order.OrderStatus.PARTIALLY_FILLED;
        } else if (amount != null && remains.compareTo(amount) < 0) {
            status = Order.OrderStatus.PARTIALLY_FILLED;
        } else if (amount != null && remains.compareTo(amount) == 0) {
            status = Order.OrderStatus.NEW;
        } else {
            status = Order.OrderStatus.UNKNOWN;
        }
        return status;
    }
}
