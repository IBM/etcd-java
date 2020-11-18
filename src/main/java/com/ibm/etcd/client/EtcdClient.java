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
import java.util.Collections;
import java.util.List;
import java.util.Set;
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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.net.ssl.SSLException;
import javax.net.ssl.TrustManagerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.ByteSource;
import com.google.common.util.concurrent.ForwardingExecutorService;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.protobuf.ByteString;
import com.ibm.etcd.api.AuthGrpc;
import com.ibm.etcd.api.AuthenticateRequest;
import com.ibm.etcd.api.AuthenticateResponse;
import com.ibm.etcd.client.GrpcClient.AuthProvider;
import com.ibm.etcd.client.kv.EtcdKvClient;
import com.ibm.etcd.client.kv.KvClient;
import com.ibm.etcd.client.lease.EtcdLeaseClient;
import com.ibm.etcd.client.lease.LeaseClient;
import com.ibm.etcd.client.lease.PersistentLease;
import com.ibm.etcd.client.lock.EtcdLockClient;
import com.ibm.etcd.client.lock.LockClient;

import io.grpc.CallCredentials;
import io.grpc.CallOptions;
import io.grpc.Deadline;
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
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.util.AbstractReferenceCounted;
import io.netty.util.IllegalReferenceCountException;
import io.netty.util.ReferenceCounted;
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

    // (not intended to be strict hostname validation here)
    protected static final Pattern ADDR_PATT =
            Pattern.compile("(?:https?://|dns:///)?([a-zA-Z0-9\\-.]+)(?::(\\d+))?");

    private final ReferenceCounted refCount; // may be null

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
        private final List<String> endpoints;
        private boolean plainText;
        private int maxInboundMessageSize;
        private SslContextBuilder sslContextBuilder;
        private SslContext sslContext;
        private String overrideAuthority;
        private ByteString name, password;
        private long defaultTimeoutMs = DEFAULT_TIMEOUT_MS;
        private boolean preemptAuth;
        private int threads = defaultThreadCount();
        private Executor executor; // for call-backs
        private boolean sendViaEventLoop = true; // default true
        private int sessTimeoutSecs = DEFAULT_SESSION_TIMEOUT_SECS;
        private boolean refCounted;

        Builder(List<String> endpoints) {
            this.endpoints = Preconditions.checkNotNull(endpoints);
            Preconditions.checkArgument(!endpoints.isEmpty(), "empty endpoints");
        }

        /**
         * Set etcd credentials to use from {@link ByteString}s
         *
         * @param name
         * @param password
         */
        public Builder withCredentials(ByteString name, ByteString password) {
            this.name = name;
            this.password = password;
            return this;
        }

        /**
         * Set etcd credentials to use from {@link String}s as UTF-8.
         *
         * @param name
         * @param password
         */
        public Builder withCredentials(String name, String password) {
            this.name = ByteString.copyFromUtf8(name);
            this.password = ByteString.copyFromUtf8(password);
            return this;
        }

        /**
         * Attempt authentication immediately rather than if/when required.
         * Applies only if etcd credentials have been provided.
         */
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

        /**
         * Control whether all RPC requests are sent via the underlying
         * IO event loop. This helps to limit overall memory use due to
         * internal thread-local buffer pooling.
         * <p>
         * Default is <code>true</code>
         *
         * @param sendViaEventLoop
         */
        public Builder sendViaEventLoop(boolean sendViaEventLoop) {
            this.sendViaEventLoop = sendViaEventLoop;
            return this;
        }

        /**
         * Provide executor to use for user call-backs. A default
         * client-scoped executor will be used if not set.
         *
         * @param executor
         */
        public Builder withUserExecutor(Executor executor) {
            this.executor = executor;
            return this;
        }

        /**
         * Provide a default timeout to use for requests made by this client.
         * This timeout value is used per-attempt, unlike {@link Deadline}s
         * used per-call which also cover any/all retry attempts.
         * 
         * The default for this default is 10 seconds
         *
         * @param value
         * @param unit
         */
        public Builder withDefaultTimeout(long value, TimeUnit unit) {
            this.defaultTimeoutMs = TimeUnit.MILLISECONDS.convert(value, unit);
            return this;
        }

        private SslContextBuilder sslBuilder() {
            return sslContextBuilder != null ? sslContextBuilder
                    : (sslContextBuilder = GrpcSslContexts.forClient());
        }

        /**
         * Disable TLS - to connect to insecure servers in development contexts
         */
        public Builder withPlainText() {
            plainText = true;
            return this;
        }

        /**
         * Override the authority used for TLS hostname verification. Applies
         * to all endpoints and does not otherwise affect DNS name resolution.
         */
        public Builder overrideAuthority(String authority) {
            this.overrideAuthority = authority;
            return this;
        }

        /**
         * Provide CA certificate to use for TLS connection
         *
         * @param certSource
         * @throws IOException if there is an error reading from the provided {@link ByteSource}
         * @throws SSLException
         */
        public Builder withCaCert(ByteSource certSource) throws IOException, SSLException {
            try (InputStream cert = certSource.openStream()) {
                sslContext = sslBuilder().trustManager(cert).build();
            }
            return this;
        }

        /**
         * Provide custom {@link TrustManagerFactory} to use for this
         * client's TLS connection.
         *
         * @param tmf
         * @throws SSLException
         */
        public Builder withTrustManager(TrustManagerFactory tmf) throws SSLException {
            sslContext = sslBuilder().trustManager(tmf).build();
            return this;
        }

        /**
         * Configure the netty {@link SslContext} to be used by this client.
         * The provided {@link Consumer} should make updates to the passed
         * {@link SslContextBuilder} as needed, but should not call build().
         *
         * @param contextBuilder
         * @throws SSLException
         */
        public Builder withTlsConfig(Consumer<SslContextBuilder> contextBuilder) throws SSLException {
            SslContextBuilder sslBuilder = sslBuilder();
            contextBuilder.accept(sslBuilder);
            sslContext = sslBuilder.build();
            return this;
        }

        /**
         * Set the session timeout in seconds - this corresponds to the TTL of the
         * session lease, see {@link EtcdClient#getSessionLease()}.
         *
         * @param timeoutSecs
         */
        public Builder withSessionTimeoutSecs(int timeoutSecs) {
            Preconditions.checkArgument(timeoutSecs < 1, "invalid session timeout: %s", timeoutSecs);
            this.sessTimeoutSecs = timeoutSecs;
            return this;
        }

        /**
         * Set the maximum inbound message size in bytes
         *
         * @param sizeInBytes
         */
        public Builder withMaxInboundMessageSize(int sizeInBytes) {
            this.maxInboundMessageSize = sizeInBytes;
            return this;
        }

        /**
         * @return the built {@link EtcdClient} instance
         */
        public EtcdClient build() {
            NettyChannelBuilder ncb;
            if (endpoints.size() == 1) {
                ncb = NettyChannelBuilder.forTarget(endpointToUriString(endpoints.get(0)));
                if (overrideAuthority != null) {
                    ncb.overrideAuthority(overrideAuthority);
                }
            } else {
                ncb = NettyChannelBuilder
                        .forTarget(StaticEtcdNameResolverFactory.ETCD)
                        .nameResolverFactory(new StaticEtcdNameResolverFactory(
                                endpoints, overrideAuthority));
            }
            if (plainText) {
                ncb.usePlaintext();
            }
            if (sslContext != null) {
                ncb.sslContext(sslContext);
            }
            if (maxInboundMessageSize != 0) {
                ncb.maxInboundMessageSize(maxInboundMessageSize);
            }
            return new EtcdClient(ncb, defaultTimeoutMs, name, password, preemptAuth,
                    threads, executor, sendViaEventLoop, sessTimeoutSecs, refCounted);
        }
    }

    static String endpointToUriString(String endpoint) {
        Preconditions.checkNotNull(endpoint, "null endpoint");
        Matcher m = ADDR_PATT.matcher(endpoint.trim());
        if (!m.matches()) {
            throw new IllegalArgumentException("invalid endpoint: " + endpoint);
        }
        String portStr = m.group(2);
        if (portStr == null) {
            portStr = String.valueOf(DEFAULT_PORT);
        }
        return "dns:///" + m.group(1) + ":" + portStr;
    }

    private static int defaultThreadCount() {
        return Math.min(6, Runtime.getRuntime().availableProcessors());
    }

    /**
     * 
     * @param host
     * @param port
     * @return
     */
    public static Builder forEndpoint(String host, int port) {
        String target = GrpcUtil.authorityFromHostAndPort(host, port);
        return new Builder(Collections.singletonList(target));
    }

    /**
     * 
     * @param endpoints
     * @return
     */
    public static Builder forEndpoints(List<String> endpoints) {
        return new Builder(endpoints);
    }

    /**
     * 
     * @param endpoints
     * @return
     */
    public static Builder forEndpoints(String endpoints) {
        return forEndpoints(Arrays.asList(endpoints.split(",")));
    }

    EtcdClient(NettyChannelBuilder chanBuilder, long defaultTimeoutMs,
            ByteString name, ByteString password, boolean initialAuth,
            int threads, Executor userExecutor, boolean sendViaEventLoop,
            int sessTimeoutSecs, boolean refCounted) {

        if (name == null && password != null) {
            throw new IllegalArgumentException("password without name");
        }
        this.name = name;
        this.password = password;
        this.sessionTimeoutSecs = sessTimeoutSecs;

        this.refCount = !refCounted ? null : new AbstractReferenceCounted() {
            @Override
            public ReferenceCounted touch(Object hint) {
                return null;
            }
            @Override
            protected void deallocate() {
                doClose();
            }
        };
        
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

        AuthProvider authProvider = name == null ? null : new AuthProvider() {
            @Override
            public boolean requiresReauth(Throwable t) {
                return EtcdClient.reauthRequired(t);
            }
            @Override
            public CallCredentials refreshCredentials(Throwable trigger) {
                return EtcdClient.this.refreshCredentials(trigger);
            }
            @Override
            public CallCredentials refreshCredentials() {
                return refreshCredentials(null);
            }
        };

        this.sharedInternalExecutor = new SharedScheduledExecutorService(internalExecutor);
        this.grpc = new GrpcClient(channel, authProvider, sharedInternalExecutor,
                () -> Thread.currentThread() instanceof EtcdEventThread,
                userExecutor, sendViaEventLoop, defaultTimeoutMs);
        if (authProvider != null && initialAuth) {
            grpc.authenticateNow();
        }

        this.kvClient = new EtcdKvClient(grpc);
    }

    @Override
    public void close() {
        if (refCount != null) {
            refCount.release();
        } else {
            doClose();
        }
    }

    boolean retain() {
        if (refCount != null) try {
            refCount.retain();
            return true;
        } catch (IllegalReferenceCountException irce) {}
        return false;
    }

    void doClose() {
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

    private static final Set<Code> OTHER_AUTH_FAILURE_CODES = ImmutableSet.of(
            Code.INVALID_ARGUMENT, Code.FAILED_PRECONDITION, Code.PERMISSION_DENIED, Code.UNKNOWN);

    // Various different errors can imply a re-auth is required (sometimes non-obvious),
    // so we cover most related messages to be safe. This should not cause problems since
    // re-authentication attempts are rate-limited to once every 15 seconds.
    //
    // See https://godoc.org/github.com/coreos/etcd/auth#pkg-variables
    // and https://godoc.org/github.com/coreos/etcd/etcdserver/api/v3rpc/rpctypes#pkg-variables
    private static final String[] AUTH_FAILURE_MESSAGES = {
            // These will be prefixed with "etcdserver: "
            "root user does not exist",
            "root user does not have root role",
            "user name already exists",
            "user name is empty",
            "user name not found",
            "role name already exists",
            "role name not found",
            "role name is empty",
            "authentication failed, invalid user ID or password",
            "permission denied",
            "role is not granted to the user",
            "permission is not granted to the role",
            "authentication is not enabled",
            "invalid auth token",
            "invalid auth management"
    };

    private Status lastAuthFailStatus;
    private long authFailRetryTime;

    protected static boolean reauthRequired(Throwable error) {
        Status status = Status.fromThrowable(error);
        Code code = status.getCode();
        return code == Code.UNAUTHENTICATED
                || (OTHER_AUTH_FAILURE_CODES.contains(code) &&
                        (startsWith(status.getDescription(), "auth: ")
                                || endsWith(status.getDescription(), AUTH_FAILURE_MESSAGES)))
                || (code == Code.CANCELLED && reauthRequired(error.getCause()));
    }

    // This is only called in a synchronized context
    private CallCredentials refreshCredentials(Throwable trigger) {
        if (System.currentTimeMillis() < authFailRetryTime) {
            // Rate-limit how often re-auth attempts are made,
            // return last auth failure in the meantime
            return new CallCredentials() {
                @Override
                public void applyRequestMetadata(RequestInfo requestInfo,
                        Executor appExecutor, MetadataApplier applier) {
                    applier.fail(lastAuthFailStatus);
                }
                //@Override
                public void thisUsesUnstableApi() {}
            };
        }
        return new CallCredentials() {
            private volatile Metadata tokenHeader; //TODO volatile TBD
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
                        Throwable failure = ee.getCause();
                        Status failStatus;
                        if (!reauthRequired(failure)) {
                            // The authenticate RPC failed with an unexpected error (non auth-related)
                            // Ensure our top-level status remains UNAUTHENTICATED or else higher
                            // level logic will not recognize that another reauth attempt is required.
                            failStatus = Status.UNAUTHENTICATED
                                    .withDescription("(Re)authentication RPC failed")
                                    .withCause(failure);
                            authFailRetryTime = System.currentTimeMillis() + 5_000L;
                        } else {
                            failStatus = Status.fromThrowable(failure);
                            // If this was a real auth failure, postpone further attempts a bit longer
                            authFailRetryTime = System.currentTimeMillis() + 15_000L;
                        }
                        lastAuthFailStatus = failStatus;
                        // Augment with the RPC failure that triggered the re-authentication,
                        // if applicable
                        if (trigger != null) {
                            if (failStatus.getCause() != null) {
                                failStatus.getCause().addSuppressed(trigger);
                            } else {
                                failStatus = failStatus.withCause(trigger);
                            }
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
        return GrpcClient.codeFromThrowable(error) == Code.UNAVAILABLE
                && GrpcClient.isConnectException(error);
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

    protected static boolean startsWith(String str, String prefix) {
        return str != null && str.startsWith(prefix);
    }

    protected static boolean endsWith(String str, String... suffixes) {
        if (str != null) {
            for (String suffix : suffixes) {
                if (str.endsWith(suffix)) return true;
            }
        }
        return false;
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

    // Public for access by EtcdClusterConfig from config package
    public static final class Internal {
        private Internal() {}

        public static boolean retain(EtcdClient client) {
            return client != null && client.retain();
        }

        public static void cleanup(EtcdClient client) {
            if (client != null) {
                client.doClose();
            }
        }

        public static void makeRefCounted(Builder builder) {
            builder.refCounted = true;
        }
    }
}
