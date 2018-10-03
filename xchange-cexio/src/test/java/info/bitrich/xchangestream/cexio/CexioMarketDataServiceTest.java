package info.bitrich.xchangestream.cexio;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import info.bitrich.xchangestream.cexio.dto.CexioOrderBookMessage;
import info.bitrich.xchangestream.core.StreamingMarketDataService;
import io.reactivex.Observable;
import io.reactivex.observers.TestObserver;
import org.assertj.core.util.Lists;
import org.junit.Before;
import org.junit.Test;
import org.knowm.xchange.currency.CurrencyPair;
import org.knowm.xchange.dto.Order;
import org.knowm.xchange.dto.marketdata.OrderBook;
import org.knowm.xchange.dto.trade.LimitOrder;


import java.math.BigDecimal;
import java.util.ArrayList;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.assertj.core.api.Assertions.*;

public class CexioMarketDataServiceTest {

    private CexioStreamingService streamingService = mock(CexioStreamingService.class);
    private ObjectMapper objectMapper = new ObjectMapper();

    private StreamingMarketDataService marketDataService;

    @Before
    public void setUp() {
        CexioStreamingExchange exchange = new CexioStreamingExchange(streamingService);
        marketDataService = exchange.getStreamingMarketDataService();
    }

    @Test
    public void testGetOrderExecution_orderPlace() throws Exception {

        JsonNode jsonNode = objectMapper.readTree(ClassLoader.getSystemClassLoader()
                .getResourceAsStream("order-book.json"));

        when(streamingService.subscribeChannel(CexioStreamingService.MARKET_DEPTH, "pair-BTC-USD"))
                .thenReturn(Observable.just(jsonNode));

        TestObserver<OrderBook> test = marketDataService.getOrderBook(CurrencyPair.BTC_USD).test();

        LimitOrder buyOrderTemplate = new LimitOrder.Builder(Order.OrderType.BID, CurrencyPair.BTC_USD).build();
        LimitOrder sellOrderTemplate = new LimitOrder.Builder(Order.OrderType.ASK, CurrencyPair.BTC_USD).build();

        ArrayList<LimitOrder> buys = Lists.newArrayList(
                LimitOrder.Builder.from(buyOrderTemplate)
                        .limitPrice(BigDecimal.valueOf(6698))
                        .originalAmount(BigDecimal.valueOf(162700000))
                        .build(),
                LimitOrder.Builder.from(buyOrderTemplate)
                        .limitPrice(BigDecimal.valueOf(6697.9))
                        .originalAmount(BigDecimal.valueOf(100000000))
                        .build(),
                LimitOrder.Builder.from(buyOrderTemplate)
                        .limitPrice(BigDecimal.valueOf(6697.3))
                        .originalAmount(BigDecimal.valueOf(60000000))
                        .build());

        ArrayList<LimitOrder> sells = Lists.newArrayList(
                LimitOrder.Builder.from(sellOrderTemplate)
                        .limitPrice(BigDecimal.valueOf(6705.4))
                        .originalAmount(BigDecimal.valueOf(328400))
                        .build(),
                LimitOrder.Builder.from(sellOrderTemplate)
                        .limitPrice(BigDecimal.valueOf(6707.8))
                        .originalAmount(BigDecimal.valueOf(60000000))
                        .build(),
                LimitOrder.Builder.from(sellOrderTemplate)
                        .limitPrice(BigDecimal.valueOf(6707.9))
                        .originalAmount(BigDecimal.valueOf(100000000))
                        .build());

        test.assertNoErrors();
        test.assertValue(orderBook -> {
            assertThat(orderBook.getBids()).isEqualTo(buys);
            assertThat(orderBook.getAsks()).isEqualTo(sells);
            return true;
        });
    }
}
