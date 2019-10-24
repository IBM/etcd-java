/*
 * Copyright 2017, 2018 IBM Corp. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy
 * of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package com.ibm.etcd.client;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static java.util.concurrent.TimeUnit.SECONDS;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Supplier;

import javax.net.ssl.SSLException;
import javax.net.ssl.TrustManagerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.io.ByteSource;
import com.google.common.util.concurrent.ForwardingExecutorService;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.protobuf.ByteString;
import com.ibm.etcd.api.AuthGrpc;
import com.ibm.etcd.api.AuthenticateRequest;
import com.ibm.etcd.api.AuthenticateResponse;
import com.ibm.etcd.client.kv.EtcdKvClient;
import com.ibm.etcd.client.kv.KvClient;
import com.ibm.etcd.client.lease.EtcdLeaseClient;
import com.ibm.etcd.client.lease.LeaseClient;
import com.ibm.etcd.client.lease.PersistentLease;
import com.ibm.etcd.client.lock.EtcdLockClient;
import com.ibm.etcd.client.lock.LockClient;

import io.grpc.CallCredentials;
import io.grpc.CallOptions;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.Metadata.Key;
import io.grpc.MethodDescriptor;
import io.grpc.Status;
import io.grpc.Status.Code;
import io.grpc.internal.GrpcUtil;
import io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.NettyChannelBuilder;
import io.netty.channel.Channel;
import io.netty.channel.ChannelOption;
import io.netty.channel.MultithreadEventLoopGroup;
import io.netty.channel.SingleThreadEventLoop;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.util.concurrent.FastThreadLocalThread;

public class EtcdClient implements KvStoreClient {

    private static final Key<String> TOKEN_KEY =
            Key.of("token", Metadata.ASCII_STRING_MARSHALLER);

    // avoid volatile read on every invocation
    private static final MethodDescriptor<AuthenticateRequest,AuthenticateResponse> METHOD_AUTHENTICATE =
            AuthGrpc.getAuthenticateMethod();

    public static final int DEFAULT_PORT = 2379; // default etcd server port

    private static final LeaseClient CLOSED = GrpcClient.sentinel(LeaseClient.class);

    public static final long DEFAULT_TIMEOUT_MS = 10_000L; // 10sec default
    public static final int DEFAULT_SESSION_TIMEOUT_SECS = 20; // 20sec default

    private final int sessionTimeoutSecs;

    private final ByteString name, password;

    private final MultithreadEventLoopGroup internalExecutor;
    private final ScheduledExecutorService sharedInternalExecutor;
    private final GrpcClient grpc;

    private final ManagedChannel channel;

    private final EtcdKvClient kvClient;
    private volatile LeaseClient leaseClient; // lazy-instantiated
    private volatile LockClient lockClient; // lazy-instantiated
    private volatile PersistentLease sessionLease; // lazy-instantiated

    public static class Builder {
        private final NettyChannelBuilder chanBuilder;
        private SslContextBuilder sslContextBuilder;
        private ByteString name, password;
        private long defaultTimeoutMs = DEFAULT_TIMEOUT_MS;
        private boolean preemptAuth;
        private int threads = defaultThreadCount();
        private Executor executor; // for call-backs
        private boolean sendViaEventLoop = true; // default true
        private int sessTimeoutSecs = DEFAULT_SESSION_TIMEOUT_SECS;

        Builder(NettyChannelBuilder chanBuilder) {
            this.chanBuilder = chanBuilder;
        }

        public Builder withCredentials(ByteString name, ByteString password) {
            this.name = name;
            this.password = password;
            return this;
        }

        public Builder withCredentials(String name, String password) {
            this.name = ByteString.copyFromUtf8(name);
            this.password = ByteString.copyFromUtf8(password);
            return this;
        }

        public Builder withImmediateAuth() {
            preemptAuth = true;
            return this;
        }

        /**
         * subject to change - threads to use for <b>internal</b> executor
         */
        public Builder withThreadCount(int threads) {
            this.threads = threads;
            return this;
        }

        public Builder sendViaEventLoop(boolean sendViaEventLoop) {
            this.sendViaEventLoop = sendViaEventLoop;
            return this;
        }

        public Builder withUserExecutor(Executor executor) {
            this.executor = executor;
            return this;
        }

        public Builder withDefaultTimeout(long value, TimeUnit unit) {
            this.defaultTimeoutMs = TimeUnit.MILLISECONDS.convert(value, unit);
            return this;
        }

        private SslContextBuilder sslBuilder() {
            return sslContextBuilder != null ? sslContextBuilder
                    : (sslContextBuilder = GrpcSslContexts.forClient());
        }

        public Builder withPlainText() {
            chanBuilder.usePlaintext();
            return this;
        }

        public Builder withCaCert(ByteSource certSource) throws IOException, SSLException {
            try (InputStream cert = certSource.openStream()) {
                chanBuilder.sslContext(sslBuilder().trustManager(cert).build());
            }
            return this;
        }

        public Builder withTrustManager(TrustManagerFactory tmf) throws SSLException {
            chanBuilder.sslContext(sslBuilder().trustManager(tmf).build());
            return this;
        }

        public Builder withTlsConfig(Consumer<SslContextBuilder> contextBuilder) throws SSLException {
            SslContextBuilder sslBuilder = sslBuilder();
            contextBuilder.accept(sslBuilder);
            chanBuilder.sslContext(sslBuilder.build());
            return this;
        }

        public Builder withSessionTimeoutSecs(int timeoutSecs) {
            if (timeoutSecs < 1) {
                throw new IllegalArgumentException("invalid session timeout: " + timeoutSecs);
            }
            this.sessTimeoutSecs = timeoutSecs;
            return this;
        }

        public Builder withMaxInboundMessageSize(int sizeInBytes) {
            chanBuilder.maxInboundMessageSize(sizeInBytes);
            return this;
        }

        public EtcdClient build() {
            return new EtcdClient(chanBuilder, defaultTimeoutMs, name, password,
                    preemptAuth, threads, executor, sendViaEventLoop, sessTimeoutSecs);
        }
    }

    private static int defaultThreadCount() {
        return Math.min(6, Runtime.getRuntime().availableProcessors());
    }

    public static Builder forEndpoint(String host, int port) {
        String target = GrpcUtil.authorityFromHostAndPort(host, port);
        return new Builder(NettyChannelBuilder.forTarget(target));
    }

    public static Builder forEndpoints(List<String> endpoints) {
        NettyChannelBuilder ncb = NettyChannelBuilder
                .forTarget(StaticEtcdNameResolverFactory.ETCD)
                .nameResolverFactory(new StaticEtcdNameResolverFactory(endpoints));
        return new Builder(ncb);
    }

    public static Builder forEndpoints(String endpoints) {
        return forEndpoints(Arrays.asList(endpoints.split(",")));
    }

    EtcdClient(NettyChannelBuilder chanBuilder, long defaultTimeoutMs,
            ByteString name, ByteString password, boolean initialAuth,
            int threads, Executor userExecutor, boolean sendViaEventLoop,
            int sessTimeoutSecs) {

        if (name == null && password != null) {
            throw new IllegalArgumentException("password without name");
        }
        this.name = name;
        this.password = password;
        this.sessionTimeoutSecs = sessTimeoutSecs;

        chanBuilder.keepAliveTime(10L, SECONDS);
        chanBuilder.keepAliveTimeout(8L, SECONDS);

        int connTimeout = Math.min((int) defaultTimeoutMs, 6000);
        chanBuilder.withOption(ChannelOption.CONNECT_TIMEOUT_MILLIS, connTimeout);

        //TODO loadbalancer TBD
//      chanBuilder.loadBalancerFactory(RoundRobinLoadBalancerFactory.getInstance());

        ThreadFactory tfac = new ThreadFactoryBuilder().setDaemon(true)
                .setThreadFactory(EtcdEventThread::new)
                .setNameFormat("etcd-event-pool-%d").build();

        Class<? extends Channel> channelType;
        if (Epoll.isAvailable()) {
            this.internalExecutor = new EpollEventLoopGroup(threads, tfac);
            channelType = EpollSocketChannel.class;
        } else {
            this.internalExecutor = new NioEventLoopGroup(threads, tfac);
            channelType = NioSocketChannel.class;
        }

        chanBuilder.eventLoopGroup(internalExecutor).channelType(channelType);

        if (userExecutor == null) {
            //TODO default chan executor TBD
            userExecutor = Executors.newCachedThreadPool(
                    new ThreadFactoryBuilder().setDaemon(true)
                    .setNameFormat("etcd-callback-thread-%d").build());
        }
        chanBuilder.executor(userExecutor);

        this.channel = chanBuilder.build();

        Predicate<Throwable> rr = name != null ? EtcdClient::reauthRequired : null;
        Supplier<CallCredentials> rc = name != null ? this::refreshCredentials : null;

        this.sharedInternalExecutor = new SharedScheduledExecutorService(internalExecutor);
        this.grpc = new GrpcClient(channel, rr, rc, sharedInternalExecutor,
                () -> Thread.currentThread() instanceof EtcdEventThread,
                userExecutor, sendViaEventLoop, defaultTimeoutMs);
        if (initialAuth) {
            grpc.authenticateNow();
        }

        this.kvClient = new EtcdKvClient(grpc);
    }

    @Override
    public void close() {
        if (leaseClient == CLOSED) {
            return;
        }
        synchronized (this) {
            LeaseClient toClose = leaseClient;
            if (toClose == CLOSED) {
                return;
            }
            leaseClient = CLOSED;

            kvClient.close();
            if (toClose instanceof EtcdLeaseClient) {
                ((EtcdLeaseClient) toClose).close();
            }
        }
        // Wait until there are no outstanding non-future-scheduled tasks before
        // shutting down the channel. Then wait until the channel has terminated
        // and there are no outstanding non-scheduled tasks before shutting down
        // the executor.
        // Extra complexity here is needed for versions of netty < 4.1.29.Final due
        // to the fact that the (Nio|Epoll)EventLoop.pendingTasks() method would
        // block on a task dispatched to the event loop. This is no longer the case
        // in netty 4.1.29.Final onwards.
        executeWhenIdle(() -> {
            try {
                channel.shutdown().awaitTermination(2, SECONDS);
            } catch (InterruptedException e) {}
            executeWhenIdle(() -> internalExecutor.shutdownGracefully(0, 1, SECONDS));
        });
    }

    /**
     * Execute the provided task in the EventLoopGroup only once there
     * are no more running/queued tasks (but might be future scheduled tasks).
     * The key thing here is that it will continue to wait if new tasks
     * are scheduled by the already running/queued ones.
     */
    private void executeWhenIdle(Runnable task) {
        AtomicInteger remainingTasks = new AtomicInteger(-1);
        // Two "cycles" are performed, the first with remainingTasks == -1.
        // If remainingTasks > 0 after the second cycle, this method
        // is re-called recursively (in an async context)
        CyclicBarrier cb = new CyclicBarrier(internalExecutor.executorCount(), () -> {
            int rt = remainingTasks.get();
            if (rt == -1) {
                remainingTasks.incrementAndGet();
            } else if (rt > 0) {
                executeWhenIdle(task);
            } else {
                internalExecutor.execute(task);
            }
        });
        internalExecutor.forEach(ex -> ex.execute(new Runnable() {
            @Override public void run() {
                SingleThreadEventLoop stel = (SingleThreadEventLoop) ex;
                try {
                    if (stel.pendingTasks() > 0) {
                        ex.execute(this);
                    } else {
                        cb.await();
                        if (stel.pendingTasks() > 0) {
                            remainingTasks.incrementAndGet();
                        }
                        cb.await();
                    }
                } catch (InterruptedException|BrokenBarrierException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }));
    }

    public boolean isClosed() {
        return leaseClient == CLOSED;
    }

    // ------ authentication logic

    protected static boolean reauthRequired(Throwable error) {
        Code statusCode = GrpcClient.codeFromThrowable(error);
        return statusCode == Code.UNAUTHENTICATED ||
                (statusCode == Code.INVALID_ARGUMENT
                && contains(error.getMessage(), "user name is empty") ||
                (statusCode == Code.CANCELLED
                && reauthRequired(error.getCause())));
    }

    private CallCredentials refreshCredentials() {
        return new CallCredentials() {
            private Metadata tokenHeader; //TODO volatile TBD
            private final long authTime = System.currentTimeMillis();
            private final ListenableFuture<Metadata> futureTokenHeader =
                    Futures.transform(authenticate(), ar -> tokenHeader = tokenHeader(ar), directExecutor());
            @Override
            public void applyRequestMetadata(RequestInfo requestInfo,
                    Executor appExecutor, MetadataApplier applier) {
                Metadata tokHeader = tokenHeader;
                if (tokHeader != null) {
                    applier.apply(tokHeader);
                } else futureTokenHeader.addListener(() -> {
                    try {
                        applier.apply(futureTokenHeader.get());
                    } catch (ExecutionException | InterruptedException ee) { // (IE won't be thrown)
                        Status failStatus = Status.fromThrowable(ee.getCause());
                        Code code = failStatus != null ? failStatus.getCode() : null;
                        if (code != Code.INVALID_ARGUMENT
                                && (System.currentTimeMillis() - authTime) > 15_000L) {
                            // this will force another auth attempt
                            failStatus = Status.UNAUTHENTICATED.withDescription("re-attempt re-auth");
                        }
                        applier.fail(failStatus);
                    }
                }, directExecutor());
            }
            //@Override
            public void thisUsesUnstableApi() {}
        };
    }

    private static Metadata tokenHeader(AuthenticateResponse authResponse) {
        Metadata header = new Metadata();
        header.put(TOKEN_KEY, authResponse.getToken());
        return header;
    }

    private ListenableFuture<AuthenticateResponse> authenticate() {
        AuthenticateRequest request = AuthenticateRequest.newBuilder()
                .setNameBytes(name).setPasswordBytes(password).build();
        // no call creds for auth call
        CallOptions callOpts = CallOptions.DEFAULT;
        return Futures.catchingAsync(
                grpc.fuCall(METHOD_AUTHENTICATE, request, callOpts, 0L),
                Exception.class, ex -> !retryAuthRequest(ex)
                        ? Futures.immediateFailedFuture(ex)
                        : grpc.fuCall(METHOD_AUTHENTICATE, request, callOpts, 0L),
                directExecutor());
    }

    protected static boolean retryAuthRequest(Throwable error) {
        Status status = Status.fromThrowable(error);
        Code statusCode = status != null ? status.getCode() : null;
        return statusCode == Code.UNAVAILABLE && GrpcClient.isConnectException(error);
    }

    // ------ individual client getters

    @Override
    public KvClient getKvClient() {
        return kvClient;
    }

    @Override
    public LeaseClient getLeaseClient() {
        LeaseClient lc = leaseClient;
        if (lc == null) synchronized (this) {
            if ((lc = leaseClient) == null) {
                leaseClient = lc = new EtcdLeaseClient(grpc);
            }
        }
        return lc;
    }

    @Override
    public LockClient getLockClient() {
        LockClient lc = lockClient;
        if (lc == null) synchronized (this) {
            if ((lc = lockClient) == null) {
                lockClient = lc = new EtcdLockClient(grpc, this);
            }
        }
        return lc;
    }

    @Override
    public PersistentLease getSessionLease() {
        PersistentLease sl = sessionLease;
        if (sl == null) synchronized (this) {
            if ((sl = sessionLease) == null) {
                sessionLease = sl = getLeaseClient().maintain()
                        .minTtl(sessionTimeoutSecs)
                        .permanent().start();
            }
        }
        if (leaseClient == CLOSED) {
            throw new IllegalStateException("client closed");
        }
        return sl;
    }

    public Executor getExecutor() {
        return grpc.getResponseExecutor();
    }
  
    /**
     * Care should be taken not to use this executor for any blocking
     * or CPU intensive tasks.
     */
    public ScheduledExecutorService internalScheduledExecutor() {
        return sharedInternalExecutor;
    }

    // ------- utilities

    protected static <T> Predicate<T> constantPredicate(boolean val) {
        return val ? Predicates.alwaysTrue() : Predicates.alwaysFalse();
    }

    protected static boolean contains(String str, String subStr) {
        return str != null && str.contains(subStr);
    }

    protected static final class EtcdEventThread extends FastThreadLocalThread {
        public EtcdEventThread(Runnable r) {
            super(r);
        }
    }

    /**
     * Wrapper to prevent direct shutdown
     */
    static final class SharedScheduledExecutorService extends ForwardingExecutorService
        implements ScheduledExecutorService {
        private final ScheduledExecutorService delegate;

        public SharedScheduledExecutorService(ScheduledExecutorService delegate) {
            this.delegate = delegate;
        }
        @Override
        protected ExecutorService delegate() {
            return delegate;
        }
        @Override
        public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
            return delegate.schedule(command, delay, unit);
        }
        @Override
        public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit) {
            return delegate.schedule(callable, delay, unit);
        }
        @Override
        public ScheduledFuture<?> scheduleAtFixedRate(Runnable command,
                long initialDelay, long period, TimeUnit unit) {
            return delegate.scheduleAtFixedRate(command, initialDelay, period, unit);
        }
        @Override
        public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command,
                long initialDelay, long delay, TimeUnit unit) {
            return delegate.scheduleWithFixedDelay(command, initialDelay, delay, unit);
        }
        @Override
        public void shutdown() {
            throw new UnsupportedOperationException(
                    "Cannot be shut down directly, close EtcdClient instead");
        }
        @Override
        public List<Runnable> shutdownNow() {
            throw new UnsupportedOperationException(
                    "Cannot be shut down directly, close EtcdClient instead");
        }
    }

    // -----

    @VisibleForTesting
    MultithreadEventLoopGroup getInternalExecutor() {
        return internalExecutor;
    }

}
