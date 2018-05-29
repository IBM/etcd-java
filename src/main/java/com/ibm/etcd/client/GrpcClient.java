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

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.lang.reflect.Proxy;
import java.net.ConnectException;
import java.net.NoRouteToHostException;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.RateLimiter;

import io.grpc.CallCredentials;
import io.grpc.CallOptions;
import io.grpc.Deadline;
import io.grpc.ManagedChannel;
import io.grpc.MethodDescriptor;
import io.grpc.Status;
import io.grpc.Status.Code;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.ClientResponseObserver;
import io.grpc.stub.StreamObserver;
import io.netty.util.concurrent.OrderedEventExecutor;


/**
 * grpc client support - non-etcd specific
 * 
 */
public class GrpcClient {
    
    private static final Logger logger = LoggerFactory.getLogger(GrpcClient.class);
    
    private final long defaultTimeoutMs;
    
    //TODO maybe combine these into an auth provider intface
    private final Supplier<CallCredentials> refreshCreds;
    private final Predicate<Throwable> reauthRequired;
    
    private final ManagedChannel channel;
    
    protected final ListeningScheduledExecutorService ses;
    protected final Executor userExecutor;
    // returns true if the current thread belongs to the internal executor
    protected final Condition isEventThread;
    
    // if true (default) all calls to etcd will be made via the shared
    // internal executor (grpc netty eventloopgroup).
    // this ensures better concentration of ThreadLocal bytebuf caches
    // which are allocated on this path
    protected final boolean sendViaEventLoop;
    
    // limit overall rate of immediate retries after failed calls to 1/sec,
    // to avoid excessive requests when there are connection problems
    protected final RateLimiter immediateRetryLimiter = RateLimiter.create(1.0);
    
    // modified only by reauthenticate() method
    private CallOptions callOptions = CallOptions.DEFAULT; // volatile tbd - lazy probably ok
    

    //TODO whether some/all of the channel construction is moved in here
    public GrpcClient(ManagedChannel channel,
            Predicate<Throwable> reauthRequired,
            Supplier<CallCredentials> credsSupplier,
            ScheduledExecutorService executor, Condition isEventThread, 
            Executor userExecutor, boolean sendViaEventLoop, long defaultTimeoutMs) {
        Preconditions.checkArgument((reauthRequired == null) == (credsSupplier == null),
                "must supply both or neither reauth and creds");
        this.channel = channel;
        this.refreshCreds = credsSupplier;
        this.reauthRequired = reauthRequired;
        this.ses = MoreExecutors.listeningDecorator(executor);
        this.isEventThread = isEventThread;
        this.userExecutor = userExecutor;
        this.sendViaEventLoop = sendViaEventLoop;
        this.defaultTimeoutMs = defaultTimeoutMs;
    }
    
    /**
     * @deprecated use {@link #getInternalExecutor()}
     */
    @Deprecated
    public ScheduledExecutorService getExecutor() {
        return ses;
    }
    
    public ScheduledExecutorService getInternalExecutor() {
        return ses;
    }
    
    public Executor getResponseExecutor() {
        return userExecutor;
    }
    
    public void authenticateNow() {
        if(refreshCreds != null) reauthenticate(getCallOptions());
    }
    
    protected CallOptions getCallOptions() {
        return callOptions;
    }
    
    static final RetryDecision<?> IDEMP = (t,r) -> {
        Status status = Status.fromThrowable(t);
        Code code = status != null ? status.getCode() : null;
        return code == Code.UNAVAILABLE
                || (code == Code.UNKNOWN
                && status.getDescription() != null
                && status.getDescription().startsWith("Channel closed"));
        //TODO and maybe some DEADLINE_EXCEEDED cases (but not for overall deadline)
    };
    
    static final RetryDecision<?> NON_IDEMP = (t,r)
            -> codeFromThrowable(t) == Code.UNAVAILABLE && isConnectException(t);
            
    @SuppressWarnings("unchecked")
    public static final <R> RetryDecision<R> retryDecision(boolean idempotent) {
        return (RetryDecision<R>) (idempotent ? IDEMP : NON_IDEMP);
    }
    
    public <ReqT,R> ListenableFuture<R> call(MethodDescriptor<ReqT,R> method,
            ReqT request, boolean idempotent) {
        return call(method, null, request, retryDecision(idempotent), 0, false, null, 0L);
    }
    
    //TODO probably move this
    public static interface RetryDecision<ReqT> {
        boolean retry(Throwable t, ReqT request);
    }
    
    public <ReqT,R> ListenableFuture<R> call(MethodDescriptor<ReqT,R> method,
            Condition precondition, ReqT request, RetryDecision<ReqT> retry,
            boolean backoff, Deadline deadline, long timeoutMs) {
        return call(method, precondition, request, retry, 0,
                backoff, null, timeoutMs);
    }
    
    // deadline is for entire request  (including retry pauses),
    // timeout is per-attempt and 0 means not specified
    private <ReqT,R> ListenableFuture<R> call(MethodDescriptor<ReqT,R> method,
            Condition precondition, ReqT request, RetryDecision<ReqT> retry,
            int attempt, boolean backoff, Deadline deadline, long timeoutMs) {
        if(precondition != null && !precondition.satisfied()) {
            return Futures.immediateFailedFuture(new CancellationException("precondition false"));
        }
        //TODO(maybe) in delay case (attempt > 1), if "session" is inactive,
        //     skip attempt (and dont increment attempt #)
        final CallOptions baseCallOpts = getCallOptions();
        CallOptions callOpts = deadline != null ? baseCallOpts.withDeadline(deadline) : baseCallOpts;
        return Futures.catchingAsync(fuCall(method, request, callOpts, timeoutMs), Exception.class, t -> {
            boolean reauth;
            // first case: if no retries to do then fail
            if((!backoff && attempt > 0) || (!(reauth = reauthIfRequired(t, baseCallOpts))
                            && !retry.retry(t, request))) return Futures.immediateFailedFuture(t);
            // second case: immediate retry (first failure or after auth failure + reauth)
            if(reauth || attempt == 0 && immediateRetryLimiter.tryAcquire()) {
                return call(method, precondition, request, retry,
                        reauth ? attempt : 1, backoff, deadline, timeoutMs);
            }
            int nextAttempt = attempt > 0 ? attempt + 1 : 2; // skip attempt # in the rate-limited case
            // final case: retry after back-off delay
            long delay = 500L * (1L << Math.min(nextAttempt-2, 4));
            return Futures.dereference(ses.schedule(() -> call(method, precondition, request,
                    retry, nextAttempt, backoff, deadline, timeoutMs), delay, MILLISECONDS));
        });
    }
    
    //TODO(maybe) for retriable RPCs consider fail-fast first timeout and longer retry timeout
    protected <ReqT,R> ListenableFuture<R> fuCall(MethodDescriptor<ReqT,R> method, ReqT request,
            CallOptions callOptions, long timeoutMs) {
        if(timeoutMs <= 0L) timeoutMs = defaultTimeoutMs;
        if(timeoutMs > 0L) {
            Deadline deadline = callOptions.getDeadline();
            Deadline timeoutDeadline = Deadline.after(timeoutMs, MILLISECONDS);
            if(deadline == null || timeoutDeadline.isBefore(deadline)) {
                callOptions = callOptions.withDeadline(timeoutDeadline);
            }
        }
        final CallOptions callOpts = callOptions;
        return sendViaEventLoop && !isEventThread.satisfied()
                ? Futures.dereference(ses.submit(() -> fuCall(method, request, callOpts)))
                        : fuCall(method, request, callOpts);
    }
    
    protected <ReqT,R> ListenableFuture<R> fuCall(MethodDescriptor<ReqT,R> method, ReqT request,
            CallOptions callOptions) {
        return ClientCalls.futureUnaryCall(channel.newCall(method, callOptions), request);
    }
    
    protected boolean retryableStreamError(Throwable error) {
        return (Status.fromThrowable(error).getCode() != Code.INVALID_ARGUMENT
                && !causedByJavaError(error));
    }
    
    protected static boolean causedByJavaError(Throwable t) {
        return causedBy(t, Error.class);
    }
    
    /**
     * @return true if reauthentication was required and attempted
     */
    protected boolean reauthIfRequired(Throwable error, CallOptions callOpts) {
        if(reauthRequired == null || !reauthRequired.apply(error)) return false;
        reauthenticate(callOpts);
        return true;
    }
    
    public static boolean isConnectException(Throwable t) {
        return causedBy(t, ConnectException.class) || causedBy(t, NoRouteToHostException.class);
    }
    
    public static Code codeFromThrowable(Throwable t) {
        Status status = Status.fromThrowable(t);
        return status != null ? status.getCode() : null;
    }
    
    
    private void reauthenticate(CallOptions failedOpts) {
        // assert name != null && password != null;
        if(getCallOptions() != failedOpts) return; // obj identity comparison intentional
        synchronized(this) {
            CallOptions callOpts = getCallOptions();
            if(callOpts != failedOpts) return;
            callOptions = callOpts.withCallCredentials(refreshCreds.get());
        }
    }
    
    public <ReqT,RespT> StreamObserver<ReqT> callStream(MethodDescriptor<ReqT,RespT> method,
            ResilientResponseObserver<ReqT,RespT> respStream) {
        return callStream(method, respStream, null);
    }
    
    public <ReqT,RespT> StreamObserver<ReqT> callStream(MethodDescriptor<ReqT,RespT> method,
            ResilientResponseObserver<ReqT,RespT> respStream, Executor responseExecutor) {
        // must explicitly auth in stream case to ensure unauthenticated version isn't used
        if(refreshCreds != null && getCallOptions() == CallOptions.DEFAULT) authenticateNow();
        return new ResilientBiDiStream<>(method, respStream, responseExecutor).start();
    }
    
    public static interface ResilientResponseObserver<ReqT,RespT> extends StreamObserver<RespT> {
        /**
         * Called once initially, and once after each {@link #onReplaced(StreamObserver)},
         * to indicate the corresponding (sub) stream is successfully established
         */
        public void onEstablished();
        
        /**
         * Indicates the underlying stream failed and will be re-established. There is
         * no guarantee that any requests sent to the current request stream have
         * been delivered, the provided stream should be used in its place to send
         * all subsequent requests, including re-submissions if necessary. Any subsequent
         * {@link #onEstablished()} or {@link #onNext(Object)} calls received will
         * be responses from this <b>new</b> stream, it's guaranteed that there will
         * be no more from the prior stream.
         * 
         * @param newStreamRequestObserver
         */
        public void onReplaced(StreamObserver<ReqT> newStreamRequestObserver);
    }
    
    
    class ResilientBiDiStream<ReqT,RespT> {
        private final MethodDescriptor<ReqT,RespT> method;
        private final ResilientResponseObserver<ReqT,RespT> respStream;
        private final Executor responseExecutor;
        
        // null if !sendViaEventLoop
        private final Executor requestExecutor;
        
        // accessed only from response thread and retry task scheduled
        // from the onError message (prior to stream being reestablished)
        private CallOptions sentCallOptions;
        private int errCounter = 0;
        
        // provided to user, buffers and wraps real req stream when active.
        // field accessed only from response thread
        private RequestSubStream userReqStream;
        
        // finished reflects *user* closing stream via terminal method (not incoming stream closure)
        // error == null indicates complete versus failed when finished == true
        // modified only by response thread
        private boolean finished;
        private Throwable error;
        
        /**
         * 
         * @param method
         * @param respStream
         * @param responseExecutor must be serialized
         */
        public ResilientBiDiStream(MethodDescriptor<ReqT,RespT> method,
                ResilientResponseObserver<ReqT,RespT> respStream,
                Executor responseExecutor) {
            this.method = method;
            this.respStream = respStream;
            this.responseExecutor = serialized(responseExecutor != null
                    ? responseExecutor : userExecutor, 0);
            this.requestExecutor = !sendViaEventLoop ? null : serialized(ses, 0);
        }
        
        // must only be called once - enforcement logic omitted since private
        StreamObserver<ReqT> start() {
            RequestSubStream firstStream = new RequestSubStream();
            userReqStream = firstStream;
            responseExecutor.execute(this::refreshBackingStream);
            return firstStream;
        }
        
        class RequestSubStream implements StreamObserver<ReqT> {
            // lifecycle: null -> real stream -> EMPTY_STREAM
            private volatile StreamObserver<ReqT> grpcReqStream; // only modified by response thread
            // grpcReqStream non-null => preConnectBuffer null
            private Queue<ReqT> preConnectBuffer;
            
            // called by user thread
            @Override
            public void onNext(ReqT value) {
                if(finished) return; // illegal usage
                StreamObserver<ReqT> rs = grpcReqStream;
                if(rs == null) synchronized(this) {
                    rs = grpcReqStream;
                    if(rs == null) {
                        if(preConnectBuffer == null) {
                            preConnectBuffer = new ArrayDeque<>(8); // bounded TBD
                        }
                        preConnectBuffer.add(value);
                        return;
                    }
                }
                
                if(requestExecutor == null) rs.onNext(value); // (***)
                else {
                    final StreamObserver<ReqT> rsFinal = rs;
                    requestExecutor.execute(() -> rsFinal.onNext(value));
                }
            }
            
            // called by user thread
            @Override
            public void onError(Throwable t) {
                onFinish(t);
            }
            // called by user thread
            @Override
            public void onCompleted() {
               onFinish(null);
            }
            
            // called from response thread
            boolean established(StreamObserver<ReqT> stream) {
                StreamObserver<ReqT> curStream = grpcReqStream;
                if(curStream == null) synchronized(this) {
                    Queue<ReqT> pcb = preConnectBuffer;
                    if(pcb != null) {
                        ReqT req;
                        while((req = pcb.poll()) != null) stream.onNext(req);
                        preConnectBuffer = null;
                    }
                    initialReqStream = null;
                    if(finished) grpcReqStream = emptyStream();
                    else {
                        grpcReqStream = stream;
                        return true;
                    }
                }
                else if(stream == curStream) return false;
                
                // here either finished or it's an unexpected new stream
                if(!finished) {
                    logger.info("Closing unexpected new stream of method "
                            + method.getFullMethodName());
                }
                closeStream(stream, error);
                return false;
            }
            
            boolean isEstablished() {
                return grpcReqStream != null;
            }
            
            // called from grpc response thread
            void discard(Throwable err) {
                StreamObserver<ReqT> curStream = grpcReqStream, empty = emptyStream();
                if(curStream == empty) return;
                if(curStream == null) synchronized(this) {
                    grpcReqStream = empty;
                    preConnectBuffer = null;
                }
                //TODO this *could* overlap with an in-progress
                //   onNext (***) above in the sendViaEventLoop == false case, but unlikely
                // for now, delay sending the close to further minimize the chance
                else close(err, false);
            }
            
            // called from grpc response thread
            void close(Throwable err, boolean fromUser) {
                StreamObserver<ReqT> curStream = grpcReqStream, empty = emptyStream();
                if(curStream == null || curStream == empty) return;
                grpcReqStream = empty;
                //assert preConnectBuffer == null;
                if(fromUser) closeStream(curStream, err);
                else {
                    Runnable closeTask = () -> closeStream(curStream, err);
                    if(requestExecutor != null) requestExecutor.execute(closeTask);
                    else ses.schedule(closeTask, 400, MILLISECONDS);
                }
            }
        }
        
        // called by user thread
        private void onFinish(Throwable err) {
            if(finished) return; // shouldn't be called more than once anyhow
            responseExecutor.execute(() -> {
                if(finished) return;
                
                // don't treat this as final if authentication
                // is enabled and the error reflects that reauth
                // is required - instead just cancel the "current"
                // stream which will cause the top-level stream
                // to be refreshed after a reauth is done
                if(err == null || reauthRequired == null
                        || !reauthRequired.apply(err)) {
                    error = err;
                    finished = true;
                }
                userReqStream.close(err, true);
            });
        }
        
        /*
         * We assume the caller (grpc) abides by StreamObserver contract
         */
        private final StreamObserver<RespT> respWrapper = new ClientResponseObserver<ReqT,RespT>() {
            @Override public void beforeStart(ClientCallStreamObserver<ReqT> rs) {
                rs.setOnReadyHandler(() -> {
                    // called from grpc response thread
                    if(rs.isReady()) {
                        errCounter = 0;
                        boolean notify = userReqStream.established(rs);
                        if(notify) respStream.onEstablished();
                    }
                });
            }
            // called from grpc response thread
            @Override public void onNext(RespT value) {
                respStream.onNext(value);
            }
            // called from grpc response thread
            @Override public void onError(Throwable t) {
                boolean finalError, reauthed = false;
                if(finished) finalError = true;
                else {
                    reauthed = reauthIfRequired(t, sentCallOptions);
                    finalError = !reauthed && !retryableStreamError(t);
                }
                if(!finalError) {
                    int errCount = ++errCounter;
                    String msg = "retryable onError #"+errCount
                            +" on underlying stream of method "+method.getFullMethodName();
                    if(logger.isDebugEnabled()) logger.info(msg, t);
                    else logger.info(msg+": "+t.getClass().getName()+": "+t.getMessage());
                    
                    RequestSubStream userStreamBefore = userReqStream;
                    if(userStreamBefore.isEstablished()) {
                        userReqStream = new RequestSubStream();
                        userStreamBefore.discard(null);
                        
                        // must call onReplaced prior to refreshing the stream, otherwise
                        // the response observer may be called with responses from the
                        // new stream prior to onReplaced returning
                        respStream.onReplaced(userReqStream);
                    }
                    else if(initialReqStream != null) {
                        // else no need to replace user stream, but cancel outbound stream
                        initialReqStream.onError(t);
                        initialReqStream = null;
                    }

                    // re-attempt immediately after reauthentication
                    if(reauthed || (errCount <= 1 && immediateRetryLimiter.tryAcquire())) {
                        refreshBackingStream();
                    }
                    else {
                        if(errCount == 1) errCount++;
                        // delay stream retry - first time random delay
                        // between 0.5 and 1.5sec, then 2, 4, 8 secs
                        long delay = 500L + (errCount == 2 ?
                                ThreadLocalRandom.current().nextLong(1000L)
                                : 2000L * (1 << Math.min(errCount-3, 2)));
                        ses.schedule(ResilientBiDiStream.this::refreshBackingStream,
                                delay, TimeUnit.MILLISECONDS);
                    }
                } else {
                    sentCallOptions = null;
                    userReqStream.discard(t);
                    respStream.onError(t);
                }
            }
            // called from grpc response thread
            @Override public void onCompleted() {
                if(!finished) {
                    logger.warn("Unexpected onCompleted received"
                            + " for stream of method "+method.getFullMethodName());
                  //TODO(maybe) reestablish stream in this case?
                }
                sentCallOptions = null;
                userReqStream.discard(null);
                respStream.onCompleted();
            }
        };
        
        // called only from:
        // - grpc response thread
        // - scheduled retry (no active stream)
        private void refreshBackingStream() {
            if(finished) return;
            CallOptions callOpts = getCallOptions();
            sentCallOptions = callOpts;
            callOpts = callOpts.withExecutor(responseExecutor);
            initialReqStream = ClientCalls.asyncBidiStreamingCall(channel
                    .newCall(method, callOpts), respWrapper);
        }
        
        // this is just stored to cancel if the call fails before
        // being established
        private StreamObserver<ReqT> initialReqStream;
    }
    
    
    // ------- utilities
    
    public static <T> T waitFor(Future<T> fut) {
        return waitFor(fut, -1L);
    }
    
    public static <T> T waitFor(Future<T> fut, long timeoutMillis) {
        try {
            return timeoutMillis < 0L ? fut.get() : fut.get(timeoutMillis, MILLISECONDS);
        } catch(InterruptedException|CancellationException e) {
            fut.cancel(true);
            throw Status.CANCELLED.withCause(e).asRuntimeException();
        } catch(ExecutionException ee) {
            throw Status.fromThrowable(ee.getCause()).asRuntimeException();
        } catch(TimeoutException te) {
            fut.cancel(true);
            throw Status.DEADLINE_EXCEEDED.withCause(te)
            .withDescription("local timeout of "+timeoutMillis+"ms exceeded")
            .asRuntimeException();
        } catch(RuntimeException rte) {
            fut.cancel(true);
            throw Status.fromThrowable(rte).asRuntimeException();
        }
    }
    
    protected static void closeStream(StreamObserver<?> stream, Throwable err) {
        if(err == null) stream.onCompleted();
        else stream.onError(err);
    }
    
    @SuppressWarnings("rawtypes")
    private static final StreamObserver<?> EMPTY_STREAM = new StreamObserver() {
        @Override public void onCompleted() {}
        @Override public void onError(Throwable t) {}
        @Override public void onNext(Object value) {}
    };
    
    @SuppressWarnings("unchecked")
    protected static <ReqT> StreamObserver<ReqT> emptyStream() {
        return (StreamObserver<ReqT>) EMPTY_STREAM;
    }

    protected static <T> Predicate<T> constantPredicate(boolean val) {
        return val ? Predicates.<T>alwaysTrue() : Predicates.<T>alwaysFalse();
    }
    
    protected static boolean contains(String str, String subStr) {
        return str != null && str.contains(subStr);
    }
    
    public static boolean causedBy(Throwable t, Class<? extends Throwable> exClass) {
        return t != null && (exClass.isAssignableFrom(t.getClass())
                || causedBy(t.getCause(), exClass));
    }
    
    @SuppressWarnings("unchecked")
    public static <I> I sentinel(Class<I> intface) {
        return (I) Proxy.newProxyInstance(intface.getClassLoader(),
                new Class<?>[] {intface}, (p,m,a) -> {
                    if("toString".equals(m.getName())) return "SENTINEL";
                    if("hashCode".equals(m.getName())) return System.identityHashCode(p);
                    if("equals".equals(m.getName())) return a[0] == p;
                    throw new IllegalStateException("attempt to invoke sentinel");
                });
    }
    
    public static Executor serialized(Executor parent, int bufferSize) {
        return parent instanceof SerializingExecutor
                || parent instanceof io.grpc.internal.SerializingExecutor
                || parent == MoreExecutors.directExecutor()
                || parent instanceof OrderedEventExecutor
                ? parent : new SerializingExecutor(parent, bufferSize);
    }
}
