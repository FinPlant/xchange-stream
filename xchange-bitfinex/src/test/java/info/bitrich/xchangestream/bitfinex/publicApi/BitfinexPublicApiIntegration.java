package info.bitrich.xchangestream.bitfinex.publicApi;

import info.bitrich.xchangestream.bitfinex.BitfinexStreamingExchange;
import info.bitrich.xchangestream.bitfinex.BitfinexStreamingMarketDataService;
import info.bitrich.xchangestream.bitfinex.dto.BitfinexOrderbookUpdate;
import info.bitrich.xchangestream.core.StreamingExchange;
import info.bitrich.xchangestream.core.StreamingExchangeFactory;
import io.reactivex.observers.TestObserver;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.knowm.xchange.currency.CurrencyPair;
import org.knowm.xchange.dto.marketdata.OrderBook;
import org.knowm.xchange.dto.marketdata.OrderBookUpdate;

import java.util.concurrent.TimeUnit;

public class BitfinexPublicApiIntegration {
    private StreamingExchange streamingExchange;
    private BitfinexStreamingMarketDataService streamingMarketDataService;

    @Before
    public void setup() {
        streamingExchange = StreamingExchangeFactory.INSTANCE.createExchange(BitfinexStreamingExchange.class.getName());
        streamingExchange.connect().blockingAwait();
        streamingMarketDataService =
                (BitfinexStreamingMarketDataService) streamingExchange.getStreamingMarketDataService();
    }

    @After
    public void cleanup() {
        if(streamingExchange.isAlive()){
            streamingExchange.disconnect().blockingAwait();
        }
    }

    @Test
    public void getOrderBookUpdates() {
        TestObserver<OrderBookUpdate> observer =
                streamingMarketDataService.getOrderBookUpdates(CurrencyPair.BTC_USD, 100).take(5).test();

        observer.awaitTerminalEvent(5, TimeUnit.SECONDS);
        observer.assertNoErrors();
        observer.assertValueCount(5);
    }

    @Test
    public void getOrderBook() {
        TestObserver<OrderBook> observer =
                streamingMarketDataService.getOrderBook(CurrencyPair.BTC_USD, 100).take(5).test();

        observer.awaitTerminalEvent(5, TimeUnit.SECONDS);
        observer.assertNoErrors();
        observer.assertValueCount(5);
    }
}