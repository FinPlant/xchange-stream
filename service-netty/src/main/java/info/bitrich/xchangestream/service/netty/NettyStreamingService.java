package info.bitrich.xchangestream.service.netty;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import info.bitrich.xchangestream.service.netty.strategy.HeartbeatStrategy;
import info.bitrich.xchangestream.service.netty.strategy.DefaultHeartbeatStrategy;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.websocketx.*;
import io.reactivex.*;
import io.reactivex.disposables.Disposable;
import io.reactivex.subjects.PublishSubject;
import io.reactivex.subjects.Subject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.websocketx.extensions.WebSocketClientExtensionHandler;
import io.netty.handler.codec.http.websocketx.extensions.compression.WebSocketClientCompressionHandler;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.reactivex.subjects.BehaviorSubject;

public abstract class NettyStreamingService<T> {
    private final Logger LOG = LoggerFactory.getLogger(this.getClass());

    public static final Duration DEFAULT_CONNECTION_TIMEOUT = Duration.ofSeconds(10);
    public static final Duration DEFAULT_RETRY_DURATION = Duration.ofSeconds(15);

    private class Subscription {
        final Subject<T> subject;
        final String channelName;
        final Object[] args;

        Subscription(Subject<T> subject, String channelName, Object[] args) {
            this.subject = subject;
            this.channelName = channelName;
            this.args = args;
        }
    }

    private final int maxFramePayloadLength;
    private final URI uri;
    private final Duration retryDuration;
    private final Duration connectionTimeout;
    private final HeartbeatStrategy heartbeatStrategy;

    protected final Map<String, Subscription> channels = new ConcurrentHashMap<>();

    private final BehaviorSubject<Boolean> connectedSubject = BehaviorSubject.createDefault(false);
    private final AtomicBoolean isAllowReconnect = new AtomicBoolean(false);

    private NioEventLoopGroup eventLoopGroup;
    private Disposable resubscribeDisposable;
    private Disposable pingDisposable;
    private boolean compressedMessages = false;
    private Channel webSocketChannel;

    public NettyStreamingService(String apiUrl) {
        this(apiUrl, 65536);
    }

    public NettyStreamingService(String apiUrl, int maxFramePayloadLength) {
        this(apiUrl, maxFramePayloadLength, DEFAULT_CONNECTION_TIMEOUT, DEFAULT_RETRY_DURATION,
                new DefaultHeartbeatStrategy());
    }

    public NettyStreamingService(String apiUrl, int maxFramePayloadLength, Duration connectionTimeout,
                                 Duration retryDuration, HeartbeatStrategy heartbeatStrategy) {
        try {
            this.maxFramePayloadLength = maxFramePayloadLength;
            this.retryDuration = retryDuration;
            this.heartbeatStrategy = heartbeatStrategy;
            this.connectionTimeout = connectionTimeout;
            this.uri = new URI(apiUrl);
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("Error parsing URI " + apiUrl, e);
        }
    }

    public Completable connect() {
        return connectImpl().doOnComplete(() -> isAllowReconnect.set(true));
    }

    public Completable reconnect() {
        return connectImpl();
    }

    public Completable disconnect() {

        return Completable.create(completable -> {

            LOG.debug("Disconnecting...");

            isAllowReconnect.set(false);
            if (resubscribeDisposable != null) {
                resubscribeDisposable.dispose();
            }

            Runnable cleanup = () -> {
                if (eventLoopGroup != null) {
                    eventLoopGroup.shutdownGracefully();
                }
                channels.clear();
                completable.onComplete();
                onDisconnected();
            };

            if (webSocketChannel == null || !webSocketChannel.isOpen()) {
                cleanup.run();
                return;
            }

            CloseWebSocketFrame closeFrame = new CloseWebSocketFrame();
            webSocketChannel.writeAndFlush(closeFrame).addListener(future -> cleanup.run());
        });
    }

    public Observable<Boolean> connected() {
        return connectedSubject.distinctUntilChanged();
    }

    public abstract String getSubscribeMessage(String channelName, Object... args) throws IOException;

    public abstract String getUnsubscribeMessage(String channelName, Object... args) throws IOException;

    public String getSubscriptionUniqueId(String channelName, Object... args) {
        return channelName;
    }

    protected Completable authorize() {
        return Completable.complete();
    }

    /**
     * Handler that receives incoming messages.
     *
     * @param message Content of the message from the server.
     */
    protected abstract void messageHandler(String message);

    protected void messageHandler(ByteBuf message) {
        throw new IllegalStateException(String.format("Got unexpected binary message: %s", message));
    }

    public void sendMessage(String message) {
        LOG.trace("<=: {}", message);

        if (webSocketChannel == null || !webSocketChannel.isOpen()) {
            LOG.warn("WebSocket is not open! Call connect first.");
            return;
        }

        if (!webSocketChannel.isWritable()) {
            LOG.warn("Cannot send data to WebSocket as it is not writable.");
            return;
        }

        if (message != null) {
            WebSocketFrame frame = new TextWebSocketFrame(message);
            webSocketChannel.writeAndFlush(frame);
        }
    }

    public Observable<T> subscribeChannel(String channelName, Object... args) {

        final String channelId = getSubscriptionUniqueId(channelName, args);

        Subject<T> observable;

        if (!channels.containsKey(channelId)) {
            LOG.info("Subscribing to channel {}", channelId);

            PublishSubject<T> subject = PublishSubject.create();
            observable = subject;

            Subscription newSubscription = new Subscription(subject, channelName, args);
            channels.put(channelId, newSubscription);
            try {
                String message = getSubscribeMessage(channelName, args);
                if (message != null) {
                    sendMessage(message);
                }
            } catch (IOException throwable) {
                subject.onError(throwable);
            }
        } else {
            LOG.debug("Subscribing to existing channel {}", channelId);
            observable = channels.get(channelId).subject;
        }

        return observable.doAfterTerminate(() -> {

            if (!channels.containsKey(channelId)) {
                LOG.warn("Unsubscribe from nonexistent channel {}", channelId);
                return;
            }

            LOG.info("Unsubscribe from channel {}", channelId);

            String message = getUnsubscribeMessage(channelId, args);
            if (message != null) {
                sendMessage(message);
            }
            channels.remove(channelId);
        });
    }

    public boolean isSocketOpen() {
        return webSocketChannel != null && webSocketChannel.isOpen();
    }

    public void useCompressedMessages(boolean compressedMessages) {
        this.compressedMessages = compressedMessages;
    }

    protected abstract String getChannelNameFromMessage(T message) throws IOException;

    protected String getChannel(T message) {
        String channel;
        try {
            channel = getChannelNameFromMessage(message);
        } catch (Exception e) {
            LOG.error("Cannot parse channel from message: {}", message, e);
            return null;
        }
        return channel;
    }

    protected void handleMessage(T message) {

        String channel = getChannel(message);
        if (channel == null) {
            LOG.debug("Missing channel from: {}", message);
            return;
        }

        handleChannelMessage(channel, message);
    }

    protected void handleError(T message, Throwable t) {
        String channel = getChannel(message);
        handleChannelError(channel, t);
    }

    protected void handleChannelMessage(String channel, T message) {
        Subscription subscription = channels.get(channel);
        if (subscription == null) {
            LOG.debug("No subscription for channel {}.", channel);
            return;
        }

        Observer<T> observer = subscription.subject;
        if (observer == null) {
            LOG.debug("No subscriber for channel {}.", channel);
            return;
        }

        observer.onNext(message);
    }

    protected void handleChannelError(String channel, Throwable t) {
        Subscription subscription = channels.get(channel);
        if (subscription == null) {
            LOG.debug("No subscription for channel {}.", channel);
            return;
        }

        Observer<T> observer = subscription.subject;
        if (observer == null) {
            LOG.debug("No subscriber for channel {}.", channel);
            return;
        }

        observer.onError(t);
    }

    protected WebSocketClientExtensionHandler getWebSocketClientExtensionHandler() {
        return WebSocketClientCompressionHandler.INSTANCE;
    }

    protected WebSocketClientHandler getWebSocketClientHandler(
            WebSocketClientHandshaker handshaker,
            WebSocketClientHandler.WebSocketTextMessageHandler textMessageHandler,
            WebSocketClientHandler.WebSocketBinaryMessageHandler binaryMessageHandler) {

        return new NettyWebSocketClientHandler(handshaker, textMessageHandler, binaryMessageHandler);
    }

    protected class NettyWebSocketClientHandler extends WebSocketClientHandler {

        protected NettyWebSocketClientHandler(WebSocketClientHandshaker handshaker,
                                              WebSocketTextMessageHandler textMessageHandler,
                                              WebSocketBinaryMessageHandler binaryMessageHandler) {
            super(handshaker, textMessageHandler, binaryMessageHandler);
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) {

            onDisconnected();

            if (isAllowReconnect.get()) {
                super.channelInactive(ctx);

                if (resubscribeDisposable != null && !resubscribeDisposable.isDisposed()) {
                    LOG.info("Connection closed by the host at reconnect");
                    return;
                }

                LOG.info("Reopening websocket because it was closed by the host");

                resubscribeDisposable = reconnect()
                        .doOnError(t -> LOG.warn("Problem with reconnect: {}", t.getMessage(), t))
                        .retryWhen(new RetryWithDelay(retryDuration.toMillis()))
                        .subscribe(() -> {
                            LOG.info("Resubscribing channels");
                            resubscribeChannels();
                        });
            }
        }
    }

    private Completable connectImpl() {
        return Completable.create(emitter -> {
            try {
                LOG.info("Connecting to {}://{}:{}{}", uri.getScheme(), uri.getHost(), uri.getPort(), uri.getPath());
                String scheme = uri.getScheme() == null ? "ws" : uri.getScheme();

                String host = uri.getHost();
                if (host == null) {
                    throw new IllegalArgumentException("Host cannot be null.");
                }

                final int port;
                if (uri.getPort() == -1) {
                    if ("ws".equalsIgnoreCase(scheme)) {
                        port = 80;
                    } else if ("wss".equalsIgnoreCase(scheme)) {
                        port = 443;
                    } else {
                        port = -1;
                    }
                } else {
                    port = uri.getPort();
                }

                if (!"ws".equalsIgnoreCase(scheme) && !"wss".equalsIgnoreCase(scheme)) {
                    throw new IllegalArgumentException("Only WS(S) is supported.");
                }

                final boolean ssl = "wss".equalsIgnoreCase(scheme);
                final SslContext sslCtx;
                if (ssl) {
                    sslCtx = SslContextBuilder.forClient().build();
                } else {
                    sslCtx = null;
                }

                final WebSocketClientHandler handler = getWebSocketClientHandler(WebSocketClientHandshakerFactory.newHandshaker(
                        uri, WebSocketVersion.V13, null, true, new DefaultHttpHeaders(), maxFramePayloadLength),
                        this::messageHandler, this::messageHandler);

                this.eventLoopGroup = new NioEventLoopGroup();

                Bootstrap b = new Bootstrap();
                b.group(eventLoopGroup)
                        .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, java.lang.Math.toIntExact(connectionTimeout.toMillis()))
                        .channel(NioSocketChannel.class)
                        .handler(new ChannelInitializer<SocketChannel>() {
                            @Override
                            protected void initChannel(SocketChannel ch) {
                                ChannelPipeline p = ch.pipeline();
                                if (sslCtx != null) {
                                    p.addLast(sslCtx.newHandler(ch.alloc(), host, port));
                                }

                                WebSocketClientExtensionHandler clientExtensionHandler = getWebSocketClientExtensionHandler();
                                List<ChannelHandler> handlers = new ArrayList<>(4);
                                handlers.add(new HttpClientCodec());
                                if (compressedMessages) handlers.add(WebSocketClientCompressionHandler.INSTANCE);
                                handlers.add(new HttpObjectAggregator(65536));

                                if (clientExtensionHandler != null) {
                                    handlers.add(clientExtensionHandler);
                                }

                                handlers.add(handler);
                                p.addLast(handlers.toArray(new ChannelHandler[handlers.size()]));
                            }
                        });

                b.connect(uri.getHost(), port).addListener((ChannelFuture future) -> {
                    webSocketChannel = future.channel();
                    if (future.isSuccess()) {
                        handler.handshakeFuture().addListener(f -> {
                            if (f.isSuccess()) {
                                emitter.onComplete();
                            } else {
                                emitter.onError(f.cause());
                                onDisconnected();
                            }
                        });
                    } else {
                        emitter.onError(future.cause());
                        onDisconnected();
                    }

                });
            } catch (Exception throwable) {
                emitter.onError(throwable);
            }
        }).andThen(authorize())
          .doOnComplete(this::onConnected);
    }

    private void onDisconnected() {
        connectedSubject.onNext(false);

        if (pingDisposable != null) {
            pingDisposable.dispose();
        }

        LOG.debug("Disconnected");
    }

    private void onConnected() {
        connectedSubject.onNext(true);

        if (heartbeatStrategy != null) {
            long period = heartbeatStrategy.getPeriod().getSeconds();
            pingDisposable = Observable.interval(period, TimeUnit.SECONDS).subscribe(unused -> {
                sendPing(heartbeatStrategy.getHeartbeatFrame());
            });
        }
    }

    private void resubscribeChannels() {
        for (String channelId : channels.keySet()) {
            try {
                Subscription subscription = channels.get(channelId);
                String message = getSubscribeMessage(subscription.channelName, subscription.args);
                if (message != null) {
                    sendMessage(message);
                }
            } catch (IOException e) {
                LOG.error("Failed to reconnect channel: {}", channelId);
            }
        }
    }

    private void sendPing(WebSocketFrame frame) {
        LOG.trace("Sending ping: {}", frame);

        if (webSocketChannel == null || !webSocketChannel.isOpen()) {
            LOG.warn("Cannot send ping. WebSocket is not open! Call connect first.");
            return;
        }

        if (!webSocketChannel.isWritable()) {
            LOG.warn("Cannot send data to WebSocket as it is not writable.");
            return;
        }

        webSocketChannel.writeAndFlush(frame);
    }
}
