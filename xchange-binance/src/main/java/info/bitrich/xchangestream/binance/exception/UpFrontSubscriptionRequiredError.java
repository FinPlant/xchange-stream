package info.bitrich.xchangestream.binance.exception;

public class UpFrontSubscriptionRequiredError extends UnsupportedOperationException {
    public UpFrontSubscriptionRequiredError() {
        super("Binance exchange only supports up front subscriptions - subscribe at connect time");
    }
}
