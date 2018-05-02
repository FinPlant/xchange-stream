package info.bitrich.xchangestream.cexio;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import info.bitrich.xchangestream.cexio.dto.CexioWebSocketTransaction;
import io.reactivex.observers.TestObserver;
import org.junit.Before;
import org.junit.Test;
import org.knowm.xchange.currency.CurrencyPair;
import org.knowm.xchange.dto.Order;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.Date;

public class CexioStreamingServiceTest {

    private CexioStreamingExchange cexioStreamingExchange;

    @Before
    public void setUp() {
        cexioStreamingExchange = new CexioStreamingExchange();
    }

    @Test
    public void testGetOrderExecution_orderPlace() throws Exception {
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode jsonNode = objectMapper.readTree(ClassLoader.getSystemClassLoader()
                .getResourceAsStream("order-place.json"));

        CexioStreamingPrivateDataRawService service =
                (CexioStreamingPrivateDataRawService) cexioStreamingExchange.getStreamingPrivateDataService();

        TestObserver<Order> test = service.getOrders().test();

        service.handleMessage(jsonNode);

        CexioOrder expected = new CexioOrder(Order.OrderType.BID, CurrencyPair.BTC_USD, new BigDecimal("0.002"),
                "5913254239", new Date(1522135708956L), new BigDecimal("7176.5"),
                new BigDecimal("0.16"), Order.OrderStatus.NEW);
        test.assertValue(expected);
    }

    @Test
    public void testGetOrderExecution_orderFill() throws Exception {
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode jsonNode = objectMapper.readTree(ClassLoader.getSystemClassLoader()
                .getResourceAsStream("order-fill.json"));

        CexioStreamingPrivateDataRawService service =
                (CexioStreamingPrivateDataRawService) cexioStreamingExchange.getStreamingPrivateDataService();

        TestObserver<Order> test = service.getOrders().test();

        service.handleMessage(jsonNode);

        CexioOrder expected = new CexioOrder(CurrencyPair.BTC_USD, "5891752542", Order.OrderStatus.FILLED,
                BigDecimal.ZERO);
        test.assertValue(expected);
    }

    @Test
    public void testGetOrderExecution_orderPartialFill() throws Exception {
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode jsonNode = objectMapper.readTree(ClassLoader.getSystemClassLoader()
                .getResourceAsStream("order-partial-fill.json"));

        CexioStreamingPrivateDataRawService service =
                (CexioStreamingPrivateDataRawService) cexioStreamingExchange.getStreamingPrivateDataService();


        TestObserver<Order> test = service.getOrders().test();

        service.handleMessage(jsonNode);

        CexioOrder expected = new CexioOrder(Order.OrderType.ASK, CurrencyPair.BTC_USD, new BigDecimal("1.91342713"),
                "6035463456", new Date(1523973448227L), new BigDecimal("782"),
                new BigDecimal("0.15"), Order.OrderStatus.PARTIALLY_FILLED);

        test.assertValue(expected);
    }

    @Test
    public void testGetOrderExecution_orderCancel() throws Exception {
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode jsonNode = objectMapper.readTree(ClassLoader.getSystemClassLoader()
                .getResourceAsStream("order-cancel.json"));

        CexioStreamingPrivateDataRawService service =
                (CexioStreamingPrivateDataRawService) cexioStreamingExchange.getStreamingPrivateDataService();


        TestObserver<Order> test = service.getOrders().test();

        service.handleMessage(jsonNode);

        CexioOrder expected = new CexioOrder(CurrencyPair.BTC_USD,
                "5891717811",
                Order.OrderStatus.CANCELED,
                new BigDecimal("0.002"));
        test.assertValue(expected);
    }

    @Test
    public void testGetOrderExecution_invalidJson() throws Exception {
        CexioStreamingPrivateDataRawService service =
                (CexioStreamingPrivateDataRawService) cexioStreamingExchange.getStreamingPrivateDataService();


        TestObserver<Order> test = service.getOrders().test();

        service.messageHandler("wrong");

        test.assertError(IOException.class);
    }

    @Test
    public void testGetTransaction_orderPlace() throws Exception {
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode jsonNode = objectMapper.readTree(ClassLoader.getSystemClassLoader()
                .getResourceAsStream("transaction-place.json"));

        CexioStreamingPrivateDataRawService service =
                (CexioStreamingPrivateDataRawService) cexioStreamingExchange.getStreamingPrivateDataService();


        TestObserver<CexioWebSocketTransaction> test = service.getTransactions().test();

        service.handleMessage(jsonNode);

        CexioWebSocketTransaction transaction = new CexioWebSocketTransaction(
                "5915157030",
                "order:5915157028:a:USD",
                "user:up118134628:a:USD",
                new BigDecimal("0.02"),
                new BigDecimal("16.40"),
                new BigDecimal("35.24"),
                "up118134628",
                "USD",
                null,
                new BigDecimal("-16.40"),
                5915157028L,
                null,
                null,
                null,
                "buy",
                Date.from(Instant.parse("2018-03-27T15:16:52.016Z")),
                new BigDecimal("35.24"),
                null,
                null, null, null, null);

        test.assertValue(transaction);
    }

    @Test
    public void testGetTransaction_orderExecute() throws Exception {
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode jsonNode = objectMapper.readTree(ClassLoader.getSystemClassLoader()
                .getResourceAsStream("transaction-exec.json"));

        CexioStreamingPrivateDataRawService service =
                (CexioStreamingPrivateDataRawService) cexioStreamingExchange.getStreamingPrivateDataService();

        TestObserver<CexioWebSocketTransaction> test = service.getTransactions().test();

        service.handleMessage(jsonNode);

        CexioWebSocketTransaction transaction = new CexioWebSocketTransaction(
                "5918682827",
                "order:5918682821:a:BTC",
                "user:up118134628:a:BTC",
                new BigDecimal("0.00200000"),
                new BigDecimal("0"),
                new BigDecimal("0.00600000"),
                "up118134628",
                "BTC",
                "USD",
                new BigDecimal("0.00200000"),
                5918682821L,
                5918682821L,
                5918682779L,
                new BigDecimal("8030"),
                "buy",
                Date.from(Instant.parse("2018-03-28T05:41:49.482Z")),
                new BigDecimal("0.00600000"),
                new BigDecimal("0.05"),
                null, null, null, null);

        test.assertValue(transaction);
    }

    @Test
    public void testGetTransaction_balanceOperation() throws Exception {
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode jsonNode = objectMapper.readTree(ClassLoader.getSystemClassLoader()
                .getResourceAsStream("transaction-deposit.json"));

        CexioStreamingPrivateDataRawService service =
                (CexioStreamingPrivateDataRawService) cexioStreamingExchange.getStreamingPrivateDataService();

        TestObserver<CexioWebSocketTransaction> test = service.getTransactions().test();

        service.handleMessage(jsonNode);

        CexioWebSocketTransaction transaction = new CexioWebSocketTransaction(
                "6124119108",
                null,
                null,
                null,
                null,
                null,
                "up12345678",
                "BTC",
                null,
                new BigDecimal("0.65304468"),
                null,
                null,
                null,
                null,
                "deposit",
                Date.from(Instant.parse("2018-05-01T15:56:46.428Z")),
                null,
                null,
                "4496e500c15aa3bf3a06e451b4bfa8ba8d59f46fc420191e2eeee3b6c9eae605",
                0L, "pending", "3Mm1kFuN6AGQq3pUWAezfY3fF1C46WVFcm");

        test.assertValue(transaction);
    }
}
