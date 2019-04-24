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
package com.ibm.etcd.client.lease;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static com.ibm.etcd.client.lease.PersistentLease.LeaseState.ACTIVE;
import static com.ibm.etcd.client.lease.PersistentLease.LeaseState.ACTIVE_NO_CONN;
import static com.ibm.etcd.client.lease.PersistentLease.LeaseState.CLOSED;
import static com.ibm.etcd.client.lease.PersistentLease.LeaseState.EXPIRED;
import static com.ibm.etcd.client.lease.PersistentLease.LeaseState.PENDING;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.ibm.etcd.api.LeaseGrantRequest;
import com.ibm.etcd.api.LeaseGrantResponse;
import com.ibm.etcd.api.LeaseGrpc;
import com.ibm.etcd.api.LeaseKeepAliveRequest;
import com.ibm.etcd.api.LeaseKeepAliveResponse;
import com.ibm.etcd.api.LeaseLeasesRequest;
import com.ibm.etcd.api.LeaseLeasesResponse;
import com.ibm.etcd.api.LeaseRevokeRequest;
import com.ibm.etcd.api.LeaseRevokeResponse;
import com.ibm.etcd.api.LeaseTimeToLiveRequest;
import com.ibm.etcd.api.LeaseTimeToLiveResponse;
import com.ibm.etcd.client.FluentRequest.AbstractFluentRequest;
import com.ibm.etcd.client.FutureListener;
import com.ibm.etcd.client.GrpcClient;
import com.ibm.etcd.client.GrpcClient.ResilientResponseObserver;
import com.ibm.etcd.client.lease.PersistentLease.LeaseState;

import io.grpc.MethodDescriptor;
import io.grpc.StatusRuntimeException;
import io.grpc.Status.Code;
import io.grpc.stub.StreamObserver;

/**
 * 
 */
public final class EtcdLeaseClient implements LeaseClient, Closeable {
    
    private static final Logger logger = LoggerFactory.getLogger(EtcdLeaseClient.class);
    
    private static final Exception CANCEL_EXCEPTION = new CancellationException();
    
    // avoid volatile read on every invocation
    private static final MethodDescriptor<LeaseGrantRequest,LeaseGrantResponse> METHOD_LEASE_GRANT =
            LeaseGrpc.getLeaseGrantMethod();
    private static final MethodDescriptor<LeaseRevokeRequest,LeaseRevokeResponse> METHOD_LEASE_REVOKE =
            LeaseGrpc.getLeaseRevokeMethod();
    private static final MethodDescriptor<LeaseTimeToLiveRequest,LeaseTimeToLiveResponse> METHOD_LEASE_TIME_TO_LIVE =
            LeaseGrpc.getLeaseTimeToLiveMethod();
    private static final MethodDescriptor<LeaseKeepAliveRequest,LeaseKeepAliveResponse> METHOD_LEASE_KEEP_ALIVE =
            LeaseGrpc.getLeaseKeepAliveMethod();
    private static final MethodDescriptor<LeaseLeasesRequest,LeaseLeasesResponse> METHOD_LEASE_LEASES =
            LeaseGrpc.getLeaseLeasesMethod();

    private final GrpcClient client;
    
    private final ScheduledExecutorService ses;
    
    private volatile boolean closed;
    
    public EtcdLeaseClient(GrpcClient client) {
        this.client = client;
        this.ses = client.getInternalExecutor();
        this.kaReqExecutor = GrpcClient.serialized(ses);
        this.respExecutor = GrpcClient.serialized(ses);
    }
    
    // ------ simple lease APIs
    
    @Deprecated
    @Override
    public ListenableFuture<LeaseGrantResponse> create(long leaseId, long ttlSecs) {
        return client.call(METHOD_LEASE_GRANT, LeaseGrantRequest.newBuilder()
                .setID(leaseId).setTTL(ttlSecs).build(), false);
    }
    
    final class EtcdGrantRequest extends AbstractFluentRequest<FluentGrantRequest,
        LeaseGrantRequest,LeaseGrantResponse,LeaseGrantRequest.Builder> implements FluentGrantRequest {

        EtcdGrantRequest(long ttl) {
            super(EtcdLeaseClient.this.client, LeaseGrantRequest.newBuilder().setTTL(ttl));
        }
        @Override
        protected MethodDescriptor<LeaseGrantRequest, LeaseGrantResponse> getMethod() {
            return METHOD_LEASE_GRANT;
        }
        @Override
        protected boolean idempotent() {
            return false;
        }
        @Override
        public FluentGrantRequest leaseId(long leaseId) {
            builder.setID(leaseId);
            return this;
        }
    }
    
    @Override
    public FluentGrantRequest grant(long ttlSecs) {
        return new EtcdGrantRequest(ttlSecs);
    }
    
    @Override
    public ListenableFuture<LeaseRevokeResponse> revoke(long leaseId) {
        return client.call(METHOD_LEASE_REVOKE,
                LeaseRevokeRequest.newBuilder().setID(leaseId).build(), false);
    }
    
    @Override
    public ListenableFuture<LeaseRevokeResponse> revoke(long leaseId, boolean ensureWithRetries) {
        return !ensureWithRetries ? revoke(leaseId) : Futures.catching(client.call(METHOD_LEASE_REVOKE,
                null, LeaseRevokeRequest.newBuilder().setID(leaseId).build(), null,
                (t,r) -> !isNotFound(t) && !closed, true, null, 0L),
                StatusRuntimeException.class, sre -> {
                    if(sre.getStatus().getCode() != Code.NOT_FOUND) throw sre;
                    return LeaseRevokeResponse.getDefaultInstance();
                }, directExecutor());
    }
    
    @Override
    public ListenableFuture<LeaseTimeToLiveResponse> ttl(long leaseId, boolean includeKeys) {
        return client.call(METHOD_LEASE_TIME_TO_LIVE,
                LeaseTimeToLiveRequest.newBuilder().setID(leaseId)
                .setKeys(includeKeys).build(), true);
    }
    
    final class KeepAliveFuture extends AbstractFuture<LeaseKeepAliveResponse> {
        final long leaseId;
        public KeepAliveFuture(long leaseId) {
            this.leaseId = leaseId;
        }
        @Override
        protected boolean set(LeaseKeepAliveResponse value) {
            return super.set(value);
        }
        @Override
        protected void interruptTask() {
            Long lid = leaseId;
            for(;;) {
                List<KeepAliveFuture> futs = oneTimeMap.get(lid);
                if(futs == null || !futs.contains(this)) return;
                if(futs.size() == 1) {
                    if(oneTimeMap.remove(lid, futs)) {
                        leaseRemoved();
                        return;
                    }
                } else if(oneTimeMap.replace(lid, futs, futs.stream()
                        .filter(this::equals).collect(Collectors.toList()))) return;
            }
        }
    }
    
    @Override
    public ListenableFuture<LeaseKeepAliveResponse> keepAliveOnce(long leaseId) {
        Long lid = leaseId;
        final KeepAliveFuture ourFut = new KeepAliveFuture(lid);
        List<KeepAliveFuture> futs = oneTimeMap.get(lid), newFuts;
        for(;;) if(futs == null) {
            newFuts = Collections.singletonList(ourFut);
            if((futs = oneTimeMap.putIfAbsent(lid, newFuts)) == null) {
                leaseAdded();
                sendKeepAlive(leaseId);
                return ourFut;
            }
        } else {
            newFuts = new ArrayList<>(futs.size() +1);
            newFuts.addAll(futs);
            newFuts.add(ourFut);
            if(oneTimeMap.replace(lid, futs, newFuts)) return ourFut;
            futs = oneTimeMap.get(lid);
        }
    }
    
    @Override
    public ListenableFuture<LeaseLeasesResponse> list() {
        return client.call(METHOD_LEASE_LEASES, LeaseLeasesRequest.getDefaultInstance(), true);
    }
    
    // ------ persistent lease impl
    
    /* Still TODO:
     *   - support shared lease maintenance
     *   - (maybe) custom per-observer executors
     */
    
    protected static final int MIN_MIN_EXPIRY_SECS = 2;
    protected static final int MIN_INTERVAL_SECS = 4;
    
    protected static final int DEFAULT_MIN_EXPIRY_SECS = 10;
    protected static final int DEFAULT_INTERVAL_SECS = 5;
    
    // only used from request executor
    protected final LeaseKeepAliveRequest.Builder KAR_BUILDER = LeaseKeepAliveRequest.newBuilder();
    protected StreamObserver<LeaseKeepAliveRequest> kaReqStream;
    protected int leaseCount;
    
    protected final Executor kaReqExecutor, respExecutor;
    
    protected final Set<LeaseRecord> allLeases = ConcurrentHashMap.newKeySet();
    protected final ConcurrentMap<Long,LeaseRecord> leaseMap = new ConcurrentHashMap<>();
    protected final ConcurrentMap<Long,List<KeepAliveFuture>> oneTimeMap
        = new ConcurrentHashMap<>(4);
    
    @Override
    public FluentMaintainRequest maintain() {
        return new FluentMaintainRequest() {
            private long id;
            private int intervalSecs = DEFAULT_INTERVAL_SECS;
            private int minTtlSecs = DEFAULT_MIN_EXPIRY_SECS;
            private boolean permanent;
            private Executor executor;
            
            @Override
            public FluentMaintainRequest leaseId(long leaseId) {
                if(leaseId < 0L) throw new IllegalArgumentException("invalid leaseId "+leaseId);
                this.id = leaseId;
                return this;
            }
            @Override
            public FluentMaintainRequest keepAliveFreq(int frequencySecs) {
                if(frequencySecs < MIN_INTERVAL_SECS) {
                    throw new IllegalArgumentException("invalid keep-alive freq "+frequencySecs);
                }
                this.intervalSecs = frequencySecs;
                return this;
            }
            @Override
            public FluentMaintainRequest minTtl(int minTtlSecs) {
                if(minTtlSecs < MIN_MIN_EXPIRY_SECS) {
                    throw new IllegalArgumentException("invalid min expiry "+minTtlSecs);
                }
                this.minTtlSecs = minTtlSecs;
                return this;
            }
            @Override
            public FluentMaintainRequest executor(Executor executor) {
                this.executor = executor;
                return this;
            }
            @Override
            public FluentMaintainRequest permanent() {
                this.permanent = true;
                return this;
            }
            @Override
            public PersistentLease start(StreamObserver<LeaseState> observer) {
                return newPersisentLease(id, minTtlSecs, intervalSecs,
                        observer, executor, permanent);
            }
            @Override
            public PersistentLease start() {
                return start(null);
            }
        };
    }
    
    // throws IllegalStateException
    protected PersistentLease newPersisentLease(long leaseId, int minExpirySecs, int keepAliveFreqSecs,
            StreamObserver<LeaseState> observer, Executor executor, boolean protect) {
        if(closed) throw new IllegalStateException("client closed");
        LeaseRecord rec = !protect ? new LeaseRecord(leaseId, minExpirySecs,
                keepAliveFreqSecs, observer, executor)
                : new ProtectedLeaseRecord(leaseId, minExpirySecs,
                        keepAliveFreqSecs, observer, executor);
        if(leaseId != 0L && leaseMap.putIfAbsent(leaseId, rec) != null)
            throw new IllegalStateException("duplicate lease id");

        boolean ok = false;
        try {
            leaseAdded();
            respExecutor.execute(() -> rec.start(streamEstablished));
            ok = true;
        } finally {
            if(!ok) leaseMap.remove(leaseId, rec);
        }
        return rec;
    }
    
    private void leaseAdded() {
        kaReqExecutor.execute(() -> {
            if(leaseCount++ != 0) return;
            kaReqStream = client.callStream(METHOD_LEASE_KEEP_ALIVE,
                    responseObserver, respExecutor);
        });
    }
    
    private void leaseRemoved() {
        kaReqExecutor.execute(() -> {
            if(--leaseCount == 0) {
                //TODO probably later change to use ClientCallStreamObserver.cancel()
                kaReqStream.onError(CANCEL_EXCEPTION);
                kaReqStream = null;
            }
        });
    }
    
    // called from event loop
    protected void leaseClosed(LeaseRecord rec) {
        allLeases.remove(rec);
        if(rec.leaseId != 0L) leaseMap.remove(rec.leaseId, rec);
        leaseRemoved();
    }
    
    protected void sendKeepAlive(long leaseId) {
        kaReqExecutor.execute(() -> {
            StreamObserver<LeaseKeepAliveRequest> stream = kaReqStream;
            if(stream != null) stream.onNext(KAR_BUILDER.setID(leaseId).build());
        });
    }
    
    public void close() {
        if(closed) return;
        closed = true;
        for(LeaseRecord rec : allLeases) rec.doClose(); // this is async
        for (Iterator<List<KeepAliveFuture>> it = oneTimeMap.values().iterator(); it.hasNext();) {
            for(KeepAliveFuture fut : it.next()) fut.cancel(true);
            it.remove();
            leaseRemoved();
        }
    }
    
    boolean streamEstablished = false; // accessed only from responseExecutor
    
    protected final ResilientResponseObserver<LeaseKeepAliveRequest,LeaseKeepAliveResponse> responseObserver =
            new ResilientResponseObserver<LeaseKeepAliveRequest,LeaseKeepAliveResponse>() {

        @Override
        public void onEstablished() {
            streamEstablished = true;
            for(Long lid : oneTimeMap.keySet()) sendKeepAlive(lid);
            for(LeaseRecord rec : allLeases) rec.reconnected();
        }
        @Override
        public void onReplaced(StreamObserver<LeaseKeepAliveRequest> newStream) {
            streamEstablished = false;
            kaReqExecutor.execute(() -> { kaReqStream = newStream; });
            for(LeaseRecord rec : allLeases) rec.connectionLost();
        }
        @Override
        public void onNext(LeaseKeepAliveResponse lkar) {
            Long lid = lkar.getID();
            List<KeepAliveFuture> oneTimeFuts = oneTimeMap.remove(lid);
            if(oneTimeFuts != null) {
                leaseRemoved();
                //TODO guard against set() throwing?
                for(KeepAliveFuture fut : oneTimeFuts) fut.set(lkar);
            }
            LeaseRecord rec = leaseMap.get(lid);
            if(rec != null) rec.processKeepAliveResponse(lkar);
        }

        @Override
        public void onError(Throwable t) {
            if(closed || GrpcClient.causedBy(t, CancellationException.class)) return;
            
            streamEstablished = false;
            //TODO review fatal cases
            kaReqExecutor.execute(() -> {
                StreamObserver<LeaseKeepAliveRequest> stream = kaReqStream;
                if(stream != null) kaReqStream.onError(t);
            });
        }
        @Override
        public void onCompleted() {
            streamEstablished = false;
            // alldone
        }
    };
    
    class LeaseRecord extends AbstractFuture<Long> implements PersistentLease {
        final CopyOnWriteArrayList<StreamObserver<LeaseState>> observers;
        final Executor eventLoop, observerExecutor;
        final int intervalSecs, minExpirySecs;
        
        // all state is modified only prior to starting or in event loop
        ListenableFuture<LeaseGrantResponse> createFuture;
        
        long leaseId; // zero or final
        long keepAliveTtlSecs = -1L;
        long expiryTimeMs = -1L;
        boolean connected; //TBC
        LeaseState state = PENDING;
        
        public LeaseRecord(long leaseId,
                int minExpirySecs, int intervalSecs,
                StreamObserver<LeaseState> observer, Executor executor) {
            this.minExpirySecs = minExpirySecs;
            this.intervalSecs = intervalSecs;
            this.leaseId = leaseId;
            this.observers = observer == null ? new CopyOnWriteArrayList<>()
                    : new CopyOnWriteArrayList<>(Collections.singletonList(observer));
            this.eventLoop = GrpcClient.serialized(ses);
            this.observerExecutor = GrpcClient.serialized(executor != null ?
                    executor : client.getResponseExecutor());
        }
        
        @Override
        public void addStateObserver(StreamObserver<LeaseState> observer, boolean publishInit) {
            if(publishInit) eventLoop.execute(() -> {
                LeaseState stateNow = state;
                observers.add(observer);
                observerExecutor.execute(() -> callObserverOnNext(observer, stateNow));
            });
            else observers.add(observer);
        }
        
        @Override
        public void removeStateObserver(StreamObserver<LeaseState> observer) {
            observers.remove(observer);
        }
        
        private boolean callObserverOnNext(StreamObserver<LeaseState> observer, LeaseState state) {
            try {
                observer.onNext(state);
            } catch(RuntimeException e) {
                logger.warn("state observer onNext("
                        +state+") method threw", e);
                observers.remove(observer);
                try {
                    // per StreamObserver contract
                    observer.onError(e);
                } catch(RuntimeException ee) {}
                return false;
            }
            if(state == CLOSED) try {
                observer.onCompleted();
            } catch(RuntimeException e) {
                logger.warn("state observer onComplete method threw", e);
            }
            return true;
        }
        
        // called from ka stream response context, *not* lease event loop
        void start(boolean connected) {
            this.connected = connected;
            if(closed) close();
            else {
                allLeases.add(this);
                // if leaseId != 0, will already be in leaseMap
                create();
            }
        }
        
        // *not* called from event loop
        void reconnected() {
            eventLoop.execute(() -> {
                if(state == CLOSED) return;
                connected = true;
                if(createFuture != null) return;
                if(leaseId == 0L || state == PENDING
                        || state == EXPIRED) {
                    create();
                } else {
                    sendKeepAliveIfNeeded();
                    changeState(ACTIVE);
                }
            });
        }
        
        // called from event loop
        private void processGrantResponse(LeaseGrantResponse lgr) {
            if(leaseId == 0L) {
                leaseId = lgr.getID();
                leaseMap.put(leaseId, this);
            }
            processTtlFromServer(lgr.getTTL());
        }
        
        // *not* called from event loop
        void processKeepAliveResponse(LeaseKeepAliveResponse lkar) {
            eventLoop.execute(() -> processTtlFromServer(lkar.getTTL()));
        }
        
        // called from event loop
        private void processTtlFromServer(long newTtl) {
            if(state == CLOSED) return;
            // assert leaseId == lkar.getID();
            if(newTtl <= 0L) {
                // lease not found, trigger create
                expiryTimeMs = 0L;
                keepAliveTtlSecs = 0L;
                changeState(EXPIRED);
                create();
            } else {
                updateKeepAlive(newTtl);
                changeState(ACTIVE);
            }
        }
        
        // *not* called from event loop
        void connectionLost() {
            eventLoop.execute(() -> {
                connected = false;
                // wait 1.5 sec before exposing disconnection externally
                ses.schedule(() -> {
                    eventLoop.execute(() -> {
                        if(connected) return;
                        long ttlSecs = getCurrentTtlSecs(); 
                        LeaseState newState = ttlSecs > 0 ? ACTIVE_NO_CONN : EXPIRED;
                        changeState(newState); // won't change if pending
                        if(ttlSecs > 0) {
                            ses.schedule(this::checkExpired, ttlSecs, SECONDS);
                            //TODO(maybe) cancel on ttl update? prob no
                        }
                    });
                }, 1500L, MILLISECONDS);
            });
        }
        
        // called from timer
        void checkExpired() {
            eventLoop.execute(() -> {
                if(state != EXPIRED && getCurrentTtlSecs() <= 0) {
                    changeState(EXPIRED);
                }
            });
        }
        
        // called from event loop
        private void changeState(LeaseState targetState) {
            LeaseState stateBefore = state;
            if(stateBefore == targetState) return;
            if(stateBefore == PENDING
                    && targetState != ACTIVE
                    && targetState != CLOSED) return;
            if(stateBefore == CLOSED) return; //tbc
            
            state = targetState; // change state before completing future
            long leaseToSet = stateBefore == PENDING && targetState == ACTIVE ? leaseId : -1L;

            Iterator<StreamObserver<LeaseState>> snapshot = !observers.isEmpty()
                    ? observers.iterator() : Collections.emptyIterator();
            
            // use observerExecutor to complete future and/or call observers
            if(snapshot.hasNext() || leaseToSet != -1L) observerExecutor.execute(() -> {
                if(leaseToSet != -1L) set(leaseToSet);
                while(snapshot.hasNext()) callObserverOnNext(snapshot.next(), targetState);
            });
        }
        
        // called from start method and event loop
        void create() {
            if(createFuture != null || state == CLOSED) return;
            long ttlSecs = minExpirySecs + intervalSecs;
            createFuture = client.call(METHOD_LEASE_GRANT, () -> state != CLOSED,
                    LeaseGrantRequest.newBuilder().setID(leaseId).setTTL(ttlSecs).build(),
                    eventLoop, (t,r) -> {
                        Code code = GrpcClient.codeFromThrowable(t);
                        return connected && code != Code.ALREADY_EXISTS
                                && code != Code.FAILED_PRECONDITION;
                    },
                    true, null, 0L); //TODO timeout TBD
            Futures.addCallback(createFuture, (FutureListener<LeaseGrantResponse>) (result,t) -> {
                createFuture = null;
                if(state == CLOSED) {
                    if(t == null || !GrpcClient.isConnectException(t)) revoke();
                }
                else if(t != null) {
                    Code code = GrpcClient.codeFromThrowable(t);
                    if(code == Code.ALREADY_EXISTS || code == Code.FAILED_PRECONDITION) {
                        // precon message is "etcdserver: lease already exists"
                        // assert leaseId != 0L
                        sendKeepAliveIfNeeded();
                    }
                }
                else processGrantResponse(result);
            }, directExecutor());
        }

        // called from event loop, only for ttl > 0, 
        private void updateKeepAlive(long newTtlSecs) {
            keepAliveTtlSecs = newTtlSecs;
            expiryTimeMs = System.currentTimeMillis() + 1000L * newTtlSecs;
            if(newTtlSecs <= intervalSecs) {
                logger.warn("Keepalive ttl too short to meet target interval of "
                        +intervalSecs+" for lease "+leaseId);
            }
            //TODO(maybe) or use MIN_INTERVAL_SECS here
            long ttNextKaSecs = Math.max(intervalSecs, newTtlSecs - minExpirySecs);
            ses.schedule(() -> eventLoop.execute(this::sendKeepAliveIfNeeded),
                    ttNextKaSecs, TimeUnit.SECONDS);
        }
        
        // called from event loop
        private void sendKeepAliveIfNeeded() {
            if(connected && state != CLOSED && leaseId > 0L
                    && getCurrentTtlSecs() <= minExpirySecs) {
                sendKeepAlive(leaseId);
            }
        }
        
        @Override
        protected void interruptTask() {
            // called if initial future is successfully cancelled
            doClose();
        }
        
        @Override
        public void close() {
            doClose();
        }
        
        void doClose() {
            if(state != CLOSED) eventLoop.execute(() -> {
                if(state == CLOSED) return;
                changeState(CLOSED);
                if(createFuture == null) revoke();
                else {
                    // revoke will be done when future completes
                    createFuture.cancel(false);
                }
                leaseClosed(this);
                if(!isDone()) observerExecutor.execute(()
                        -> setException(new IllegalStateException("closed")));
            });
        }
        
        // called from event loop
        private void revoke() {
            ListenableFuture<LeaseRevokeResponse> fut = client.call(METHOD_LEASE_REVOKE,
                    () -> (leaseId != 0 && getCurrentTtlSecs() > 0), // request precondition
                    LeaseRevokeRequest.newBuilder().setID(leaseId).build(), eventLoop,
                    (t,r) -> !isNotFound(t) && !closed, // retry condition
                    true, null, 5000L);
            Futures.addCallback(fut, (FutureListener<LeaseRevokeResponse>) (v,t) -> {
                if(t == null || isNotFound(t)) {
                    expiryTimeMs = 0L;
                    keepAliveTtlSecs = 0L;
                }
            }, directExecutor());
        }
        
        //TODO TBD external getter field visibility/mutual consistency
        
        @Override
        public long getCurrentTtlSecs() {
            long expires = expiryTimeMs;
            if(expires <= 0L) return expires;
            expires -= System.currentTimeMillis();
            return expires < 0L ? 0L : expires/1000L;
        }
        
        @Override
        public long getLeaseId() {
            return leaseId;
        }

        @Override
        public LeaseState getState() {
            return state;
        }

        @Override
        public long getPreferredTtlSecs() {
            return minExpirySecs+intervalSecs;
        }

        @Override
        public long getKeepAliveTtlSecs() {
            return keepAliveTtlSecs;
        }
    }
    
    final class ProtectedLeaseRecord extends LeaseRecord {
        public ProtectedLeaseRecord(long leaseId, int minExpirySecs, int intervalSecs,
                StreamObserver<LeaseState> observer, Executor executor) {
            super(leaseId, minExpirySecs, intervalSecs, observer, executor);
        }
        
        @Override
        public boolean cancel(boolean mayInterruptIfRunning) { return false; }
        @Override
        public void close() {}
    }

    private static boolean isNotFound(Throwable t) {
        return GrpcClient.codeFromThrowable(t) == Code.NOT_FOUND;
    }
}
