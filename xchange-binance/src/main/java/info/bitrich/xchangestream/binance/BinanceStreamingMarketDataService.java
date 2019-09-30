package info.bitrich.xchangestream.binance;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import info.bitrich.xchangestream.binance.dto.*;
import info.bitrich.xchangestream.binance.exception.UpFrontSubscriptionRequiredError;
import info.bitrich.xchangestream.core.ProductSubscription;
import info.bitrich.xchangestream.core.StreamingMarketDataService;
import io.reactivex.Observable;
import io.reactivex.functions.Consumer;
import org.knowm.xchange.binance.BinanceAdapters;
import org.knowm.xchange.binance.dto.marketdata.BinanceOrderbook;
import org.knowm.xchange.binance.dto.marketdata.BinanceTicker24h;
import org.knowm.xchange.binance.service.BinanceMarketDataService;
import org.knowm.xchange.currency.CurrencyPair;
import org.knowm.xchange.dto.Order.OrderType;
import org.knowm.xchange.dto.marketdata.OrderBook;
import org.knowm.xchange.dto.marketdata.OrderBookUpdate;
import org.knowm.xchange.dto.marketdata.Ticker;
import org.knowm.xchange.dto.marketdata.Trade;
import org.knowm.xchange.exceptions.ExchangeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static info.bitrich.xchangestream.binance.dto.BaseBinanceWebSocketTransaction.BinanceWebSocketTypes.*;

public class BinanceStreamingMarketDataService implements StreamingMarketDataService {
    private static final Logger LOG = LoggerFactory.getLogger(BinanceStreamingMarketDataService.class);

    private final BinanceStreamingService service;

    private final Map<CurrencyPair, Observable<DepthBinanceWebSocketTransaction>> orderBookRawUpdatesSubscriptions;
    private final Map<CurrencyPair, Observable<BinanceTicker24h>> tickerSubscriptions;
    private final Map<CurrencyPair, Observable<OrderBook>> orderBookSubscriptions;
    private final Map<CurrencyPair, Observable<OrderBookUpdate>> orderBookUpdatesSubscriptions;
    private final Map<CurrencyPair, Observable<BinanceRawTrade>> tradeSubscriptions;
    private final ObjectMapper mapper = new ObjectMapper();
    private final BinanceMarketDataService marketDataService;

    public BinanceStreamingMarketDataService(BinanceStreamingService service,
                                             BinanceMarketDataService marketDataService) {
        this.service = service;
        this.marketDataService = marketDataService;
        this.mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        this.orderBookRawUpdatesSubscriptions = new HashMap<>();
        this.tickerSubscriptions = new HashMap<>();
        this.orderBookSubscriptions = new HashMap<>();
        this.orderBookUpdatesSubscriptions = new HashMap<>();
        this.tradeSubscriptions = new HashMap<>();
    }

    @Override
    public Observable<OrderBook> getOrderBook(CurrencyPair currencyPair, Object... args) {
        if (!service.getProductSubscription().getOrderBook().contains(currencyPair)) {
            throw new UpFrontSubscriptionRequiredError();
        }
        return orderBookSubscriptions.computeIfAbsent(currencyPair, this::createOrderBookObservable);
    }

    public Observable<BinanceTicker24h> getRawTicker(CurrencyPair currencyPair, Object... args) {
        if (!service.getProductSubscription().getTicker().contains(currencyPair)) {
            throw new UpFrontSubscriptionRequiredError();
        }
        return tickerSubscriptions.get(currencyPair);
    }

    public Observable<BinanceRawTrade> getRawTrades(CurrencyPair currencyPair, Object... args) {
        if (!service.getProductSubscription().getTrades().contains(currencyPair)) {
            throw new UpFrontSubscriptionRequiredError();
        }
        return tradeSubscriptions.get(currencyPair);
    }

    @Override
    public Observable<OrderBookUpdate> getOrderBookUpdates(CurrencyPair currencyPair, Object... args) {
        if (!service.getProductSubscription().getOrderBook().contains(currencyPair)) {
            throw new UpFrontSubscriptionRequiredError();
        }
        return orderBookUpdatesSubscriptions.computeIfAbsent(currencyPair, this::createOrderBookUpdatesObservable);
    }

    @Override
    public Observable<Ticker> getTicker(CurrencyPair currencyPair, Object... args) {
        return getRawTicker(currencyPair)
                .map(BinanceTicker24h::toTicker);
    }

    @Override
    public Observable<Trade> getTrades(CurrencyPair currencyPair, Object... args) {
        return getRawTrades(currencyPair)
                .map(rawTrade -> new Trade(
                        BinanceAdapters.convertType(rawTrade.isBuyerMarketMaker()),
                        rawTrade.getQuantity(),
                        currencyPair,
                        rawTrade.getPrice(),
                        new Date(rawTrade.getTimestamp()),
                        String.valueOf(rawTrade.getTradeId())
                ));
    }

    private static String channelFromCurrency(CurrencyPair currencyPair, String subscriptionType) {
        String currency = String.join("", currencyPair.toString().split("/")).toLowerCase();
        return currency + "@" + subscriptionType;
    }

    /**
     * Registers subscriptions with the streaming service for the given products.
     * <p>
     * As we receive messages as soon as the connection is open, we need to register subscribers to handle these before the
     * first messages arrive.
     */
    public void openSubscriptions(ProductSubscription productSubscription) {
        productSubscription.getTicker().forEach(this::initTickerSubscription);
        productSubscription.getOrderBook().forEach(this::initRawOrderBookUpdatesSubscription);
        productSubscription.getTrades().forEach(this::initTradeSubscription);
    }

    private void initTradeSubscription(CurrencyPair currencyPair) {
        tradeSubscriptions.put(currencyPair, triggerObservableBody(rawTradeStream(currencyPair)));
    }

    private void initTickerSubscription(CurrencyPair currencyPair) {
        tickerSubscriptions.put(currencyPair, triggerObservableBody(rawTickerStream(currencyPair)));
    }

    private void initRawOrderBookUpdatesSubscription(CurrencyPair currencyPair) {
        orderBookRawUpdatesSubscriptions.put(currencyPair, triggerObservableBody(rawOrderBookUpdates(currencyPair)));
    }

    private Observable<DepthBinanceWebSocketTransaction> rawOrderBookUpdates(CurrencyPair currencyPair) {
        return service.subscribeChannel(channelFromCurrency(currencyPair, "depth"))
                .map((JsonNode s) -> depthTransaction(s.toString()))
                .map(BinanceWebsocketTransaction::getData)
                .filter(data -> data.getCurrencyPair().equals(currencyPair) && data.getEventType() == DEPTH_UPDATE)
                .share();
    }

    private Observable<BinanceTicker24h> rawTickerStream(CurrencyPair currencyPair) {
        return service.subscribeChannel(channelFromCurrency(currencyPair, "ticker"))
                .map((JsonNode s) -> tickerTransaction(s.toString()))
                .filter(transaction -> transaction.getData().getCurrencyPair().equals(currencyPair) &&
                        transaction.getData().getEventType() == TICKER_24_HR)
                .map(transaction -> transaction.getData().getTicker())
                .share();
    }

    private final class OrderBookSubscription {
        final Observable<DepthBinanceWebSocketTransaction> stream;
        long snapshotlastUpdateId;
        AtomicLong lastUpdateId = new AtomicLong(0L);
        OrderBook orderBook;
        AtomicLong lastSyncTime = new AtomicLong(0L);

        private OrderBookSubscription(Observable<DepthBinanceWebSocketTransaction> stream) {
            this.stream = stream;
        }

        void invalidateSnapshot() {
            snapshotlastUpdateId = 0L;
        }

        void initSnapshotIfInvalid(CurrencyPair currencyPair) {

            if (snapshotlastUpdateId != 0L)
                return;

            // Don't attempt reconnects too often to avoid bans. 3 seconds will do it.
            long now = System.currentTimeMillis();
            if (now - lastSyncTime.get() < 3000) {
                return;
            }

            try {
                LOG.info("Fetching initial orderbook snapshot for {} ", currencyPair);
                BinanceOrderbook book = marketDataService.getBinanceOrderbook(currencyPair, 1000);
                snapshotlastUpdateId = book.lastUpdateId;
                lastUpdateId.set(book.lastUpdateId);
                orderBook = BinanceMarketDataService.convertOrderBook(book, currencyPair);
            } catch (Throwable e) {
                LOG.error("Failed to fetch initial order book for " + currencyPair, e);
                snapshotlastUpdateId = 0L;
                lastUpdateId.set(0L);
                orderBook = null;
            }
            lastSyncTime.set(now);
        }
    }

    private Observable<OrderBook> createOrderBookObservable(CurrencyPair currencyPair) {
        // 1. Open a stream to wss://stream.binance.com:9443/ws/bnbbtc@depth
        // 2. Buffer the events you receive from the stream.
        OrderBookSubscription subscription = new OrderBookSubscription(orderBookRawUpdatesSubscriptions.get(currencyPair));

        return subscription.stream

                // 3. Get a depth snapshot from https://www.binance.com/api/v1/depth?symbol=BNBBTC&limit=1000
                // (we do this if we don't already have one or we've invalidated a previous one)
                .doOnNext(transaction -> subscription.initSnapshotIfInvalid(currencyPair))

                // If we failed, don't return anything. Just keep trying until it works
                .filter(transaction -> subscription.snapshotlastUpdateId > 0L)

                // 4. Drop any event where u is <= lastUpdateId in the snapshot
                .filter(depth -> depth.getLastUpdateId() > subscription.snapshotlastUpdateId)

                // 5. The first processed should have U <= lastUpdateId+1 AND u >= lastUpdateId+1
                .filter(depth -> {
                    long lastUpdateId = subscription.lastUpdateId.get();
                    if (lastUpdateId == 0L) {
                        return depth.getFirstUpdateId() <= lastUpdateId + 1 &&
                                depth.getLastUpdateId() >= lastUpdateId + 1;
                    } else {
                        return true;
                    }
                })

                // 6. While listening to the stream, each new event's U should be equal to the previous event's u+1
                .filter(depth -> {
                    long lastUpdateId = subscription.lastUpdateId.get();
                    boolean result;
                    if (lastUpdateId == 0L) {
                        result = true;
                    } else {
                        result = depth.getFirstUpdateId() == lastUpdateId + 1;
                    }
                    if (result) {
                        subscription.lastUpdateId.set(depth.getLastUpdateId());
                    } else {
                        // If not, we re-sync.  This will commonly occur a few times when starting up, since
                        // given update ids 1,2,3,4,5,6,7,8,9, Binance may sometimes return a snapshot
                        // as of 5, but update events covering 1-3, 4-6 and 7-9.  We can't apply the 4-6
                        // update event without double-counting 5, and we can't apply the 7-9 update without
                        // missing 6.  The only thing we can do is to keep requesting a fresh snapshot until
                        // we get to a situation where the snapshot and an update event precisely line up.
                        LOG.info("OrderBook snapshot [{}] outdated (last={}, U={}, u={}). This is normal. Re-syncing.",
                                 currencyPair, lastUpdateId, depth.getFirstUpdateId(), depth.getLastUpdateId());
                        subscription.invalidateSnapshot();
                    }
                    return result;
                })

                // 7. The data in each event is the absolute quantity for a price level
                // 8. If the quantity is 0, remove the price level
                // 9. Receiving an event that removes a price level that is not in your local order book can happen and is normal.
                .map(depth -> {
                    BinanceOrderbook ob = depth.getOrderBook();
                    ob.bids.forEach((key, value) -> subscription.orderBook.update(new OrderBookUpdate(
                            OrderType.BID,
                            null,
                            currencyPair,
                            key,
                            depth.getEventTime(),
                            value)));
                    ob.asks.forEach((key, value) -> subscription.orderBook.update(new OrderBookUpdate(
                            OrderType.ASK,
                            null,
                            currencyPair,
                            key,
                            depth.getEventTime(),
                            value)));
                    return subscription.orderBook;
                }).share();
    }

    private Observable<BinanceRawTrade> rawTradeStream(CurrencyPair currencyPair) {
        return service.subscribeChannel(channelFromCurrency(currencyPair, "trade"))
                .map((JsonNode s) -> tradeTransaction(s.toString()))
                .filter(transaction ->
                                transaction.getData().getCurrencyPair().equals(currencyPair) &&
                                        transaction.getData().getEventType() == TRADE
                )
                .map(transaction -> transaction.getData().getRawTrade())
                .share();
    }

    /**
     * Force observable to execute its body, this way we get `BinanceStreamingService` to register the observables emitter
     * ready for our message arrivals.
     */
    private <T> Observable<T> triggerObservableBody(Observable<T> observable) {
        Consumer<T> NOOP = whatever -> {
        };
        observable.subscribe(NOOP);
        return observable;
    }

    private BinanceWebsocketTransaction<TickerBinanceWebsocketTransaction> tickerTransaction(String s) {
        try {
            return mapper.readValue(s, new TypeReference<BinanceWebsocketTransaction<TickerBinanceWebsocketTransaction>>() {
            });
        } catch (IOException e) {
            throw new ExchangeException("Unable to parse ticker transaction", e);
        }
    }

    private BinanceWebsocketTransaction<DepthBinanceWebSocketTransaction> depthTransaction(String s) {
        try {
            return mapper.readValue(s, new TypeReference<BinanceWebsocketTransaction<DepthBinanceWebSocketTransaction>>() {
            });
        } catch (IOException e) {
            throw new ExchangeException("Unable to parse order book transaction", e);
        }
    }

    private BinanceWebsocketTransaction<TradeBinanceWebsocketTransaction> tradeTransaction(String s) {
        try {
            return mapper.readValue(s, new TypeReference<BinanceWebsocketTransaction<TradeBinanceWebsocketTransaction>>() {
            });
        } catch (IOException e) {
            throw new ExchangeException("Unable to parse trade transaction", e);
        }
    }

    private Observable<OrderBookUpdate> createOrderBookUpdatesObservable(CurrencyPair currencyPair) {
        return orderBookRawUpdatesSubscriptions.get(currencyPair)
                .flatMapIterable(depthTransaction -> toOrderBookUpdatesList(currencyPair, depthTransaction))
                .share();
    }

    private List<OrderBookUpdate> toOrderBookUpdatesList(CurrencyPair currencyPair,
                                                         DepthBinanceWebSocketTransaction depthTransaction) {
        BinanceOrderbook orderBookDiff = depthTransaction.getOrderBook();

        Stream<OrderBookUpdate> bidStream = orderBookDiff.bids.entrySet().stream()
                .map(entry -> new OrderBookUpdate(OrderType.BID, entry.getValue(), currencyPair, entry.getKey(),
                                                  depthTransaction.getEventTime(), entry.getValue())
                );

        Stream<OrderBookUpdate> askStream = orderBookDiff.asks.entrySet().stream()
                .map(entry -> new OrderBookUpdate(OrderType.ASK, entry.getValue(), currencyPair, entry.getKey(),
                                                  depthTransaction.getEventTime(), entry.getValue())
                );

        return Stream.concat(bidStream, askStream).collect(Collectors.toList());
    }
}
