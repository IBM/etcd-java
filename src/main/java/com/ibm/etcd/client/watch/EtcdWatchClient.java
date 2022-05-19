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
package com.ibm.etcd.client.watch;

import java.io.Closeable;
import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;

import javax.annotation.concurrent.GuardedBy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.ibm.etcd.client.GrpcClient;
import com.ibm.etcd.client.GrpcClient.ResilientResponseObserver;
import com.ibm.etcd.client.kv.WatchUpdate;
import com.ibm.etcd.client.kv.KvClient.Watch;
import com.ibm.etcd.client.kv.KvClient.WatchIterator;
import com.ibm.etcd.api.ResponseHeader;
import com.ibm.etcd.api.WatchCancelRequest;
import com.ibm.etcd.api.WatchCreateRequest;
import com.ibm.etcd.api.WatchGrpc;
import com.ibm.etcd.api.WatchRequest;
import com.ibm.etcd.api.WatchResponse;

import io.grpc.MethodDescriptor;
import io.grpc.Status.Code;
import io.grpc.stub.StreamObserver;

/**
 * 
 */
public final class EtcdWatchClient implements Closeable {

    private static final Logger logger = LoggerFactory.getLogger(EtcdWatchClient.class);

    private static final Exception CANCEL_EXCEPTION = new CancellationException();

    private static final String UNAUTH_REASON_PREFIX = "rpc error: code = PermissionDenied";

    private static final MethodDescriptor<WatchRequest,WatchResponse> METHOD_WATCH =
            WatchGrpc.getWatchMethod();


    /* Watcher states:
     *   - in pendingCreate only, watchId < 0, finished == false
     *   - in activeWatchers only, watchId >= 0, finished == false
     *          (stream completion event published in this transition)
     *   - in neither, watchId < 0, finished == true
     */

    private final GrpcClient client;
    private final Executor observerExecutor; // "parent" executor
    private final Executor eventLoop; // serialized

    @GuardedBy("this")
    private StreamObserver<WatchRequest> requestStream;

    // additions done only in "this" lock, removals done only in eventLoop
    private final Queue<WatcherRecord> pendingCreate = new ConcurrentLinkedQueue<>();

    @GuardedBy("eventLoop")
    private final Map<Long,WatcherRecord> activeWatchers = new HashMap<>();


    public EtcdWatchClient(GrpcClient client) {
        this(client, client.getResponseExecutor());
    }

    public EtcdWatchClient(GrpcClient client, Executor executor) {
        this.client = client;
        this.observerExecutor = executor != null ? executor : client.getResponseExecutor();
        this.eventLoop = GrpcClient.serialized(client.getInternalExecutor());
    }

    /**
     * Internal per-watch state
     */
    final class WatcherRecord {

        private final StreamObserver<WatchUpdate> observer;
        private final WatchCreateRequest request;
        private final Executor watcherExecutor;
        private long clusterId;
        private WatchHandle creationFuture;

        long upToRevision;
        long watchId = -2L; // -2 only pre-creation, >= -1 after
        boolean lastCreateFailedAuth;
 
        boolean userCancelled, finished;
        volatile boolean vUserCancelled;

        WatcherRecord(WatchCreateRequest request,
                StreamObserver<WatchUpdate> observer,
                Executor parentExecutor, long clusterId) {
            this.observer = observer;
            this.request = request;
            long rev = request.getStartRevision();
            this.upToRevision = rev - 1;
            // bounded for back-pressure
            this.watcherExecutor = GrpcClient.serialized(parentExecutor);
            this.clusterId = clusterId;
        }

        // null => cancelled (non-error)
        public void publishCompletionEvent(final Exception err) {
            watcherExecutor.execute(() -> {
                completeCreateFuture(false, err);
                try {
                    if (err == null || vUserCancelled) {
                        observer.onCompleted();
                    } else {
                        observer.onError(err);
                    }
                } catch (RuntimeException e) {
                    logger.warn("Watch " + watchId
                            + " observer onCompleted/onError threw", e);
                }
            });
        }

        @GuardedBy("eventLoop")
        public void processWatchEvents(final WatchResponse wr) {
            if (userCancelled) {
                return; // suppress events if cancelled
            }
            int eventsCount = wr.getEventsCount();
            final long newRevision = eventsCount <= 0 ? wr.getHeader().getRevision() - 1
                    : wr.getEvents(eventsCount - 1).getKv().getModRevision();
            if (newRevision <= upToRevision) {
                return;
            }
            watcherExecutor.execute(() -> {
                try {
//                  if (first) observer.onNext(new WatchUpdate(wr.getHeader(),
//                          null, null, EventType.ESTABLISHED));
                    if (!vUserCancelled) {
                        observer.onNext(new EtcdWatchUpdate(wr));
                    }
                } catch (RuntimeException e) {
                    logger.warn("Watch observer onNext() threw (watchId = " + watchId + ")", e);

                    // must cancel the watch here per StreamObserver contract
                    cancel();
                    //TODO this will result in the watcher receiving
                    // a final onComplete, but it should really be onError in this case
                }
            });
            upToRevision = newRevision;
        }

        // returns true if addition to activeWatchers was made
        @GuardedBy("eventLoop")
        public boolean processCreatedResponse(WatchResponse wr, boolean cancelled) {
            long newWatchId = wr.getWatchId();
            long cid = wr.getHeader().getClusterId();
            if (clusterId == 0) {
                clusterId = cid;
            } else if (cid != clusterId) {
                sendCancel(newWatchId);
                processCancelledResponse(wr);
                return false;
            }
            if (cancelled || newWatchId == -1L) {
                String reason = wr.getCancelReason();
                if (reason != null && reason.startsWith(UNAUTH_REASON_PREFIX)
                        && !lastCreateFailedAuth) {
                    // If watch creation fails due to an authentication issue (likely
                    // expired), we trigger a reauth+refresh of the stream by sending
                    // an UNAUTHENTICATED error. This watch must first be re-created
                    // so that it will be retried properly in our onRefresh method.
                    synchronized (EtcdWatchClient.this) {
                        boolean notClosed = createNewWatch(this);
                        if (notClosed) {
                            StreamObserver<WatchRequest> reqStream = getRequestStream();
                            if (reqStream != null) {
                                lastCreateFailedAuth = true;
                                reqStream.onError(Code.UNAUTHENTICATED
                                    .toStatus().withDescription(reason).asException());
                            }
                        }
                    }
                } else {
                    processCancelledResponse(wr);
                }
                return false;
            }
            lastCreateFailedAuth = false;
            boolean first = this.watchId < 0, veryFirst = this.watchId == -2;
            this.watchId = newWatchId;
            if (activeWatchers.putIfAbsent(newWatchId, this) == null) {
                if (userCancelled) {
                    sendCancel(watchId);
                } else {
                    if (veryFirst) {
                        watcherExecutor.execute(() -> completeCreateFuture(true, null));
                    }
                    if (!first || wr.getEventsCount() > 0) {
                        processWatchEvents(wr);
                    }
                    return true;
                }
            } else {
                logger.error("State error: watchId conflict: " + watchId);
                //TODO cancel existing watch here?
            }
            return false;
        }

        @GuardedBy("eventLoop")
        public void processCancelledResponse(WatchResponse wr) {
            watchId = -1L;
            if (finished) {
                logger.warn("Ignoring unexpected cancel response for watchId "
                        + wr.getWatchId() + ", reason=" + wr.getCancelReason());
                return;
            }
            finished = true;
            Exception error;
            if (userCancelled) {
                error = null;
            } else {
                ResponseHeader header = wr.getHeader();
                long cancelledId = wr.getWatchId();
                if (clusterId != 0 && clusterId != header.getClusterId()) {
                    error = new ClusterChangedException(header, cancelledId, clusterId);
                } else {
                    long cRev = wr.getCompactRevision();
                    String reason = wr.getCancelReason();
                    if (cRev != 0) {
                        error = new RevisionCompactedException(header, cancelledId, reason, cRev);
                    } else if (wr.getCreated()) {
                        error = new WatchCreateException(header, cancelledId, reason);
                    } else {
                        error = new WatchCancelledException(header, cancelledId, reason);
                    }
                }
            }
            publishCompletionEvent(error);
        }

        @GuardedBy("eventLoop")
        public WatchRequest newCreateWatchRequest() {
            return WatchRequest.newBuilder()
                    .setCreateRequest(request.toBuilder()
                            .setStartRevision(upToRevision + 1)).build();
        }

        public WatchRequest firstCreateWatchRequest() {
            return WatchRequest.newBuilder()
                    .setCreateRequest(request).build();
        }

        // NOT guarded - called by user or watcherExecutor
        public void cancel() {
            if (closed || finished || userCancelled) {
                return;
            }
            eventLoop.execute(() -> {
                if (closed || userCancelled || finished) {
                    return;
                }
                sendCancel(watchId);
                vUserCancelled = userCancelled = true;
            });
        }

        private void completeCreateFuture(boolean created, Exception error) {
            WatchHandle wh = creationFuture;
            if (wh != null) {
                wh.complete(created, error);
                creationFuture = null;
            }
        }
    }

//  @Override
    public Watch watch(WatchCreateRequest createReq, StreamObserver<WatchUpdate> observer) {
        return watch(createReq, observer, null, 0);
    }

    public Watch watch(WatchCreateRequest createReq,
            StreamObserver<WatchUpdate> observer, Executor executor, long clusterId) {
        if (closed) {
            throw new IllegalStateException("closed");
        }
        final WatcherRecord wrec = new WatcherRecord(createReq,
                observer, executor != null ? executor : observerExecutor, clusterId);
        WatchHandle handle = new WatchHandle(wrec);
        wrec.creationFuture = handle;
        boolean succ = createNewWatch(wrec);
        if (!succ) {
            throw new IllegalStateException("closed");
        }
        return handle;
    }

    /**
     * Blocking watch
     */
    public WatchIterator watch(WatchCreateRequest createReq) {
        EtcdWatchIterator watchIt = new EtcdWatchIterator();
        Watch handle = watch(createReq, watchIt, MoreExecutors.directExecutor(), 0);
        return watchIt.setWatch(handle);
    }

    //TODO probably change to have a request thread instead
    private boolean createNewWatch(WatcherRecord wrec) {
        WatchRequest createReq = wrec.firstCreateWatchRequest();
        synchronized (this) {
            StreamObserver<WatchRequest> reqStream = getRequestStream();
            if (reqStream == null) {
                return false;
            }
            pendingCreate.add(wrec);
            reqStream.onNext(createReq);
        }
        return true;
    }

    static final class WatchHandle extends AbstractFuture<Boolean> implements Watch {
        private final WeakReference<WatcherRecord> wrecRef;

        WatchHandle(WatcherRecord wrec) {
            wrecRef = new WeakReference<>(wrec);
        }

        @Override
        public void close() {
            WatcherRecord wrec = wrecRef.get();
            if (wrec != null) {
                wrec.cancel();
            }
        }

        @Override
        protected void interruptTask() {
            close();
        }

        void complete(boolean created, Exception error) {
            if (error != null) {
                setException(error);
            } else {
                set(created);
            }
        }
    }

    @GuardedBy("eventLoop")
    protected void sendCancel(long watchId) {
        if (watchId < 0) {
            return; // not created yet
        }
        // send cancel request
        WatchRequest cancelReq = WatchRequest.newBuilder()
                .setCancelRequest(WatchCancelRequest.newBuilder()
                        .setWatchId(watchId).build()).build();
        synchronized (this) {
            // don't need to re-initialize reqstream if null (watch can't exist)
            StreamObserver<WatchRequest> reqStream = closed ? null : requestStream;
            if (reqStream != null) {
                reqStream.onNext(cancelReq);
            }
        }
    }

    /**
     * get current watch request stream if exists, otherwise establish new one
     */
    @GuardedBy("this")
    protected StreamObserver<WatchRequest> getRequestStream() {
        if (closed) {
            return null;
        }
        if (requestStream == null) {
            logger.debug("Watch stream starting");
            requestStream = client.callStream(METHOD_WATCH, responseObserver, eventLoop);
        }
        return requestStream;
    }

    @GuardedBy("eventLoop")
    protected void closeRequestStreamIfNoWatches() {
        synchronized (this) {
            if (requestStream != null && activeWatchers.isEmpty() && pendingCreate.isEmpty()) {
                //TODO probably later change to use ClientCallStreamObserver.cancel()
                requestStream.onError(CANCEL_EXCEPTION);
                logger.info("Watch stream cancelled due to there being no active watches");
                requestStream = null;
            }
        }
    }

    protected final ResilientResponseObserver<WatchRequest,WatchResponse> responseObserver
      = new ResilientResponseObserver<WatchRequest,WatchResponse>() {
        // all methods @GuardedBy("eventLoop")
        @Override
        public void onEstablished() {
            // nothing to do here
            logger.debug("onEstablished called for watch request stream");
        }
        @Override
        public void onNext(WatchResponse wr) {
            processResponse(wr);
        }
        @Override
        public void onReplaced(StreamObserver<WatchRequest> newStreamRequestObserver) {
            if (!closed) {
                logger.info("onReplaced called for watch request stream"
                        + (newStreamRequestObserver == null ? " with newReqStream == null" : ""));
            }
            onReplacedOrFailed(newStreamRequestObserver, null);
        }
        @Override
        public void onCompleted() {
            logger.debug("onCompleted called for watch request stream");
            // alldone
        }
        @Override
        public void onError(Throwable t) {
            logger.debug("onError called for watch request stream", t);
            if (closed || GrpcClient.causedBy(t, CancellationException.class)) {
                return;
            }
            synchronized (EtcdWatchClient.this) {
                if (closed) {
                    return;
                }
            }
            logger.warn("Unexpected fatal watch stream error", t);
            // this will cancel/complete all open user watches -
            // complete their futures exceptionally if not started,
            // and send a final onError to their stream observers
            onReplacedOrFailed(null, t instanceof Exception
                    ? (Exception) t : new RuntimeException(t));
        }

        void onReplacedOrFailed(StreamObserver<WatchRequest> newRequestStream, Exception err) {
            List<WatcherRecord> pending = null;
            synchronized (EtcdWatchClient.this) {
                requestStream = newRequestStream;
                if (!activeWatchers.isEmpty() || !pendingCreate.isEmpty()) {
                    pending = new ArrayList<>(pendingCreate);
                    pending.addAll(activeWatchers.values());
                    pendingCreate.clear();
                    activeWatchers.clear();
                }
            }

            boolean watchesExist = false;
            // recreate all the non-cancelled watches
            if (pending != null) for (WatcherRecord wrec : pending) {
                if (wrec.finished) {
                    continue;
                }
                if (wrec.watchId >= 0) {
                    wrec.watchId = -1L;
                }
                boolean cancelled = wrec.userCancelled || closed;
                if (!cancelled && newRequestStream != null) {
                    WatchRequest createReq = wrec.newCreateWatchRequest();
                    synchronized (EtcdWatchClient.this) {
                        if (closed) {
                            cancelled = true; // (client closed)
                        } else {
                            pendingCreate.add(wrec);
                            newRequestStream.onNext(createReq);
                            watchesExist = true;
                        }
                    }
                }
                // Re-check since cancelled could have been set to true within above block
                if (cancelled || newRequestStream == null) {
                    if (cancelled) {
                        wrec.vUserCancelled = wrec.userCancelled = true;
                    }
                    wrec.finished = true;
                    // err is null here in normal cancellation case
                    wrec.publishCompletionEvent(err);
                }
            }
            if (!watchesExist) {
                closeRequestStreamIfNoWatches();
            }
        }
    };

    @GuardedBy("eventLoop")
    protected void processResponse(WatchResponse wr) {
        boolean cancelled = wr.getCanceled() || wr.getCompactRevision() != 0;
        long watchId = wr.getWatchId();
        boolean watchCountReduced = false;
        WatcherRecord wrec;
        if (wr.getCreated()) {
            if (logger.isDebugEnabled()) {
                logger.debug("Watch create response received for id " + watchId);
            }
            wrec = pendingCreate.poll();
            if (wrec == null) {
                logger.error("State error: received unexpected watch create response: " + wr);
                sendCancel(wr.getWatchId()); // or maybe close/refresh stream
                //throw new IllegalStateException("unexpected watch create response");
                return;
            }
            watchCountReduced = !wrec.processCreatedResponse(wr, cancelled);
        } else if (cancelled) {
            wrec = activeWatchers.remove(watchId);
            watchCountReduced = true;
            if (wrec != null) {
                //TODO try to resume on "unexpected" cancellations?
                wrec.processCancelledResponse(wr);
            }
        } else {
            wrec = activeWatchers.get(watchId);
            if (wrec != null) {
                wrec.processWatchEvents(wr);
            } else {
                logger.warn("State error: received response for unrecognized watchId "
                        + watchId + ": " + wr);
                sendCancel(watchId); // or maybe close/refresh stream
            }
        }
        if (watchCountReduced && activeWatchers.isEmpty() && pendingCreate.isEmpty()) {
            closeRequestStreamIfNoWatches();
        }
    }

    @GuardedBy("this") // but lazy-read from other contexts
    protected boolean closed;

    @Override
    public void close() {
        if (closed) {
            return;
        }
        eventLoop.execute(() -> {
            if (!closed) synchronized (EtcdWatchClient.this) {
                if (closed) {
                    return;
                }
                closed = true;
                if (requestStream != null) {
                    //TODO probably later change to use ClientCallStreamObserver.cancel()
                    requestStream.onError(CANCEL_EXCEPTION);
                    requestStream = null;
                }
                responseObserver.onReplaced(null); // this will close any individual watches
            }
        });
    }
}
