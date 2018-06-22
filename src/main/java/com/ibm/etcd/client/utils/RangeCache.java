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
package com.ibm.etcd.client.utils;

import static com.ibm.etcd.client.GrpcClient.waitFor;

import com.ibm.etcd.api.ResponseOp;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.concurrent.GuardedBy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.ByteString;
import com.ibm.etcd.client.EtcdClient;
import com.ibm.etcd.client.GrpcClient;
import com.ibm.etcd.client.KeyUtils;
import com.ibm.etcd.client.kv.KvClient;
import com.ibm.etcd.client.kv.WatchUpdate;
import com.ibm.etcd.client.kv.KvClient.Watch;
import com.ibm.etcd.client.utils.RangeCache.Listener.EventType;
import com.ibm.etcd.client.watch.RevisionCompactedException;
import com.ibm.etcd.api.Compare;
import com.ibm.etcd.api.Compare.CompareResult;
import com.ibm.etcd.api.Compare.CompareTarget;
import com.ibm.etcd.api.CompareOrBuilder;
import com.ibm.etcd.api.DeleteRangeRequest;
import com.ibm.etcd.api.DeleteRangeResponse;
import com.ibm.etcd.api.Event;
import com.ibm.etcd.api.KeyValue;
import com.ibm.etcd.api.PutRequest;
import com.ibm.etcd.api.RangeRequest;
import com.ibm.etcd.api.RangeResponse;
import com.ibm.etcd.api.RequestOp;
import com.ibm.etcd.api.TxnRequest;
import com.ibm.etcd.api.TxnResponse;

import io.grpc.Status;
import io.grpc.Status.Code;
import io.grpc.stub.StreamObserver;

/**
 * Auto-updated write-through lock-free cache for etcd3, supporting
 * atomic compare-and-set updates.
 * <p>
 * The cache must be started via the {@link #start()} method before
 * it will populate and update itself with key/values from the
 * specified range.
 * <p>
 * Note that no range checking is done - the cache will not update
 * itself with keys outside of the specified range, but it is possible
 * to put, delete and getRemote keys outside of the range. This is not
 * advised and may result in stale out-of-range key/values remaining
 * in the cache.
 * 
 */
public class RangeCache implements AutoCloseable, Iterable<KeyValue> {

    protected static final Logger logger = LoggerFactory.getLogger(RangeCache.class);

    public static final long TIMEOUT_MS = 3500L;
    
    private final ByteString fromKey, toKey;
    
    private final transient EtcdClient client;
    private final KvClient kvClient;
    private /*final*/ Watch watch;
    
    private volatile boolean closed;
    
    @GuardedBy("this")
    private ListenableFuture<Boolean> startFuture;
    
    private final ConcurrentMap<ByteString,KeyValue> entries;
    
    // deletion queue is used to avoid race condition where a PUT is seen
    // after a delete that it precedes
    private final NavigableSet<KeyValue> deletionQueue;
    
    // non-volatile - visibility ensured via adjacent access of entries map
    private long seenUpToRev = 0L;
    
    public RangeCache(EtcdClient client, ByteString prefix) {
        this(client, prefix, false);
    }
    
    public RangeCache(EtcdClient client, ByteString prefix, boolean sorted) {
        // per etcd API spec: end == start+1 => prefix
        this(client, prefix, KeyUtils.plusOne(prefix), sorted);
    }
    
    public RangeCache(EtcdClient client, ByteString fromKey, ByteString toKey, boolean sorted) {
        this.fromKey = fromKey;
        this.toKey = toKey;
        this.client = client;
        this.kvClient = client.getKvClient();
        this.entries = !sorted ? new ConcurrentHashMap<>(32,.75f,4)
                : new ConcurrentSkipListMap<>(KeyUtils::compareByteStrings);
        this.deletionQueue = new ConcurrentSkipListSet<>((kv1, kv2) -> {
            int diff = Long.compare(kv1.getModRevision(), kv2.getModRevision());
            return diff != 0 ? diff : KeyUtils.compareByteStrings(kv1.getKey(), kv2.getKey());
        });
        this.listenerExecutor = GrpcClient.serialized(client.getExecutor(), 0);
    }
    
    /**
     * Start the cache. This must be called before the cache will
     * automatically populate and update its internal state.
     * 
     * @return a future which completes when the cache is fully
     *    initialized - i.e. its contents reflects the state of the
     *    range at or after the time this method was called
     */
    public synchronized ListenableFuture<Boolean> start() {
        if(closed) throw new IllegalStateException("closed");
        if(startFuture!=null) throw new IllegalStateException("already started");
        
        return startFuture = fullRefreshCache();
    }
    
    // internal method - should not be called while watch is active
    protected ListenableFuture<Boolean> fullRefreshCache() {
        Executor executor = MoreExecutors.directExecutor();
        ListenableFuture<List<RangeResponse>> rrfut;
        long seenUpTo = seenUpToRev;
        boolean firstTime = (seenUpTo == 0L);
        if(firstTime || entries.size() <= 20) {
            //TODO *maybe* chunking (for large caches)
            ListenableFuture<RangeResponse> rrf = kvClient.get(fromKey).rangeEnd(toKey)
                    .backoffRetry(() -> !closed)
                    .timeout(300_000L).async(); // long timeout (5min) for large ranges
            rrfut = Futures.transform(rrf, Collections::singletonList, executor);
        } else {
            // in case the local cache is large, reduce data transfer by requesting
            // just keys, and full key+value only for those modified since seenUpToRev
            RangeRequest.Builder rangeReqBld = RangeRequest.newBuilder()
                    .setKey(fromKey).setRangeEnd(toKey);
            RangeRequest newModsReq = rangeReqBld
                    .setMinModRevision(seenUpTo+1).build();
            RangeRequest otherKeysReq = rangeReqBld.clearMinModRevision()
                    .setMaxModRevision(seenUpTo).setKeysOnly(true).build();
            ListenableFuture<TxnResponse> trf = kvClient.batch()
                    .get(newModsReq).get(otherKeysReq)
                    .backoffRetry(() -> !closed)
                    .timeout(300_000L).async(); // long timeout (5min) for large ranges
            rrfut = Futures.transform(
                    trf,
                    tr -> tr.getResponsesList().stream().map(ResponseOp::getResponseRange).collect(Collectors.toList()),
                    executor);
        }
        
        return Futures.transformAsync(rrfut, rrs -> {
            if(closed) throw new CancellationException();
            Set<ByteString> snapshot = firstTime ? null : new HashSet<>();
            RangeResponse toUpdate = rrs.get(0);
            if(toUpdate.getKvsCount() > 0) for(KeyValue kv : toUpdate.getKvsList()) {
                if(!firstTime) snapshot.add(kv.getKey());
                offerUpdate(kv, true);
            }
            long snapshotRev = toUpdate.getHeader().getRevision();
            if(firstTime) notifyListeners(EventType.INITIALIZED, null, true);
            else {
                if(rrs.size() > 1) for(KeyValue kv : rrs.get(1).getKvsList()) {
                    snapshot.add(kv.getKey());
                }
                // prune deleted entries
                KeyValue.Builder kvBld = null;
                for(ByteString key : entries.keySet()) if(!snapshot.contains(key)) {
                    if(kvBld == null) kvBld = KeyValue.newBuilder()
                            .setVersion(0L).setModRevision(snapshotRev);
                    offerUpdate(kvBld.setKey(key).build(), true);
                }
            }
            revisionUpdate(snapshotRev);
            
            Watch newWatch = kvClient.watch(fromKey).rangeEnd(toKey) //.prevKv() //TODO TBD
                    .progressNotify().startRevision(snapshotRev + 1).executor(listenerExecutor)
                    .start(new StreamObserver<WatchUpdate>() {

                        @Override public void onNext(WatchUpdate update) {
                            List<Event> events = update.getEvents();
                            int eventCount = events != null ? events.size() : 0;
                            if(eventCount > 0) for(Event event : events) {
                                KeyValue kv = event.getKv();
//                              event.getPrevKv(); //TBD
                                switch(event.getType()) {
                                case DELETE:
                                    if(kv.getVersion() != 0L) kv = KeyValue.newBuilder(kv)
                                    .setVersion(0L).clearValue().build();
                                    // fall-thru
                                case PUT:
                                    offerUpdate(kv, true);
                                    break;
                                case UNRECOGNIZED: default:
                                    logger.warn("Unrecognized event for key "+kv.getKey().toStringUtf8());
                                    break;
                                }
                            }
                            revisionUpdate(eventCount == 0 ? update.getHeader().getRevision() - 1L
                                    : events.get(eventCount-1).getKv().getModRevision());
                        }
                        @Override public void onCompleted() {
                            // should only happen after external close()
                            if(!closed) {
                                if(!client.isClosed()) {
                                    logger.error("Watch completed unexpectedly (not closed)");
                                }
                                close();
                            }
                        }
                        @Override public void onError(Throwable t) {
                            logger.error("Watch failed with exception ", t);
                            if(t instanceof RevisionCompactedException) synchronized(RangeCache.this) {
                                // fail if happens during start, otherwise refresh
                                if(!closed && startFuture != null && startFuture.isDone()) {
                                    startFuture = fullRefreshCache(); // will renew watch
                                }
                            }
                        }
                    });
            synchronized(this) {
                if(closed) throw new CancellationException();
                return watch = newWatch;
            }
        }, listenerExecutor);
    }
    
    // called only from listenerExecutor context
    protected void revisionUpdate(long upToRev) {
        if(seenUpToRev >= upToRev) return;
        seenUpToRev = upToRev;
        
        // process deletion queue up to upToRec
        if(deletionQueue.isEmpty()) return;
        for(Iterator<KeyValue> it = deletionQueue.iterator();it.hasNext();) {
            KeyValue kv = it.next();
            if(kv.getModRevision() > upToRev) return;
            it.remove();
            entries.remove(kv.getKey(), kv);
        }
    }
    
    protected final List<Listener> listeners = new CopyOnWriteArrayList<>();
    
    protected final Executor listenerExecutor;
    
    //TODO maybe take optional listener-specific executor
    public void addListener(Listener listener) {
        listeners.add(listener);
    }
    
    public boolean removeListener(Listener listener) {
        return listeners.remove(listener);
    }
    
    /**
     * Interface for listening to update events from
     * the cache
     */
    @FunctionalInterface
    public static interface Listener {

        public enum EventType {
            /**
             * Key added or modified
             */
            UPDATED,
            /**
             * Key deleted
             */
            DELETED,
            /**
             * Initial cache population complete (fired only once)
             */
            INITIALIZED
        }
        
        /**
         * @param type
         * @param keyValue
         */
        public void event(EventType type, KeyValue keyValue);
    }
    
    protected void notifyListeners(EventType type, KeyValue keyValue,
            boolean inListenerExecutor) {
        
        if(!inListenerExecutor) listenerExecutor.execute(()
                -> notifyListeners(type, keyValue, true));
        else for(Listener l : listeners) try {
            l.event(type, keyValue);
        } catch(RuntimeException re) {
            logger.warn("Listener threw exception for "+type
                    +" event for key "+keyValue.getKey().toStringUtf8(), re);
        }
    }
    
    
    //------------------------------------
    
    /**
     * used to perform <b>offline</b> lease-based expiries, should
     * be called only when assumed to be disconnected and not receiving
     * watch updates
     */
    protected KeyValue offerExpiry(ByteString key) {
        // read of map is done first as a mem barrier before reading seenUpToRev
        return isDeleted(entries.get(key)) ? null : offerDelete(key, seenUpToRev+1L);
    }
    
    
    /**
     * assumed to <b>not</b> be called from watch/listener executor
     * 
     * @return <b>latest</b> value, may or may not be the provided one
     */
    protected KeyValue offerDelete(ByteString key, long modRevision) {
        return offerUpdate(KeyValue.newBuilder().setKey(key)
                .setVersion(0L).setModRevision(modRevision).build(), false);
    }
    
    /**
     * @param keyValue
     * @param watchThread if being called from background watch context
     * @return the provided value, <i>or a newer one</i> 
     */
    protected KeyValue offerUpdate(final KeyValue keyValue, boolean watchThread) {
        final long modRevision = keyValue.getModRevision();
        if(modRevision <= seenUpToRev) return kvOrNullIfDeleted(keyValue);
        final ByteString key = keyValue.getKey();
        final boolean isDeleted = isDeleted(keyValue);
        // can only do this optimization in watch context, otherwise there's
        // a possible (but unlikely) race with deletions
        if(watchThread && !isDeleted) {
            // optimized non-deletion path
            KeyValue newKv = entries.merge(key, keyValue, (k,v) ->
                (modRevision > v.getModRevision() ? keyValue : v));
            if(newKv == keyValue) notifyListeners(EventType.UPDATED, keyValue, true);
          return kvOrNullIfDeleted(newKv);
        }
        KeyValue existKv = entries.get(key);
        while(true) {
            if(existKv != null) {
                long existModRevision = existKv.getModRevision();
                if(existModRevision >= modRevision) return kvOrNullIfDeleted(existKv);
                KeyValue newKv = entries.computeIfPresent(key, (k,v) -> 
                    (existModRevision == v.getModRevision() ? keyValue : v));
                if(newKv != keyValue) {
                    existKv = newKv;
                    continue; // update failed
                }
                // update succeeded
                if(isDeleted) {
                    deletionQueue.add(keyValue);
                    if(!isDeleted(existKv)) { // previous value
                        notifyListeners(EventType.DELETED, existKv, watchThread);
                    }
                    return null;
                } else {
                    // added or updated
                    notifyListeners(EventType.UPDATED, keyValue, false);
                    return keyValue;
                }
            }
            // here existKv == null
            if(modRevision <= seenUpToRev) return null;
            if((existKv = entries.putIfAbsent(key, keyValue)) == null) {
                // update succeeded
                if(isDeleted) {
                    deletionQueue.add(keyValue);
                    return null;
                } else {
                    notifyListeners(EventType.UPDATED, keyValue, false);
                    return keyValue;
                }
            }
        }
    }
    
    protected static KeyValue kvOrNullIfDeleted(KeyValue fromCache) {
        return isDeleted(fromCache) ? null : fromCache;
    }
    
    protected static boolean isDeleted(KeyValue kv) {
        return kv == null || kv.getVersion() == 0L;
    }
    
    public KeyValue get(ByteString key) {
        return key == null ? null : kvOrNullIfDeleted(entries.get(key));
    }
    
    /**
     * @return the first KeyValue or null if empty
     */
    public KeyValue getFirst() {
        if(entries.isEmpty()) return null;
        if(entries instanceof NavigableMap) {
            @SuppressWarnings("unchecked")
            Map.Entry<ByteString,KeyValue> first
            = ((NavigableMap<ByteString,KeyValue>)entries).firstEntry();
            return first != null ? first.getValue() : null;
        }
        Iterator<KeyValue> it = entries.values().iterator();
        return it.hasNext() ? it.next() : null;
    }
    
    protected KeyValue getRemote(ByteString key, boolean weak) {
        if(key == null) return null;
        ListenableFuture<RangeResponse> rrf = kvClient.get(key)
                .serializable(weak).async();
        //TODO -- async option
        RangeResponse rr = waitFor(rrf, TIMEOUT_MS);
        KeyValue kv = rr.getCount() > 0 ? rr.getKvs(0) : null;
        return kv != null ? offerUpdate(kv, false)
                : offerDelete(key, rr.getHeader().getRevision());
    }
    
    public KeyValue getRemote(ByteString key) {
        return getRemote(key, false);
    }
    
    public KeyValue getRemoteWeak(ByteString key) {
        return getRemote(key, true);
    }
    
    public int size() {
        return entries.size() - deletionQueue.size();
    }
    
    //TODO maybe add sizeRemote() ?
    
    public boolean keyExists(ByteString key) {
        // ensures deleted records (version == 0) aren't included
        return get(key) != null;
    }
    
    public boolean keyExistsRemote(ByteString key) {
        if(key == null) return false;
        ListenableFuture<RangeResponse> rrf = kvClient.get(key).countOnly().async();
        //TODO -- async
        RangeResponse rr = waitFor(rrf, TIMEOUT_MS);
        boolean exists = rr.getCount() > 0;
        if(!exists) offerDelete(key, rr.getHeader().getRevision());
        return exists;
    }
    
    /**
     * Stores result of put operations
     */
    public static class PutResult {
        private final boolean succ;
        private final KeyValue kv;
        public PutResult(boolean success, KeyValue kv) {
            this.succ = success;
            this.kv = kv;
        }
        public boolean succ() {
            return succ;
        }
        public KeyValue kv() {
            return kv;
        }
        public KeyValue existingOrNull() {
            return succ ? null : kv;
        }
        @Override public String toString() {
            return "PutResult[succ="+succ+", kv="+kv+"]";
        }
    }
    
    /**
     * Unconditional put. Can be used to unconditionally delete
     * by passing a null value, but it's preferable to use
     * {@link #delete(ByteString)} for this.
     * 
     * @param key
     * @param value value to put
     * @return modRevision of updated keyValue
     */
    public long put(ByteString key, ByteString value) {
        return putNoGet(key, value, 0L, (CompareOrBuilder[])null);
    }
    
    /**
     * Multi-purpose put or delete, returns updated or existing KeyValue
     * 
     * @param key
     * @param value new value to put, or null to delete
     * @param lease leaseId to associate value with if successful, or 0L for no lease
     * @param conditions conditions which must all match to proceed, null or empty for unconditional
     * @return PutResult
     */
    public PutResult put(ByteString key, ByteString value, long lease,
            CompareOrBuilder... conditions) {

        ListenableFuture<TxnResponse> tf = doPut(key, value, true, lease, conditions);
        //TODO -- async option
        TxnResponse tr = waitFor(tf, TIMEOUT_MS);
        if(tr.getSucceeded()) {
            if(value != null) {
                KeyValue putValue = tr.getResponses(1).getResponseRange().getKvs(0);
                offerUpdate(putValue, false);
                return new PutResult(true, putValue);
            } else {
                offerDelete(key, tr.getHeader().getRevision());
                return new PutResult(true, null);
            }
        } else {
            // assert conditions != null && conditions.length > 0;
            RangeResponse rr = tr.getResponses(0).getResponseRange();
            KeyValue exist = rr.getKvsCount() > 0 ? offerUpdate(rr.getKvs(0), false) :
                offerDelete(key, tr.getHeader().getRevision());
            return new PutResult(false, exist);
        }
    }
    
    /**
     * @param key
     * @param value new value to put, or null to delete
     * @param modRev last-modified revision to match, or 0 for put-if-absent
     * @return PutResult
     */
    public PutResult put(ByteString key, ByteString value, long modRev) {
        // return put(key, value, new Cmp(key, Cmp.Op.EQUAL, CmpTarget.modRevision(modRev)));
        return put(key, value, 0L, modRevCompare(key, modRev));
    }
    
    /**
     * @param key
     * @param value new value to put, or null to delete
     * @param leaseId leaseId to associate value with if successful, or 0L for no lease
     * @param modRev last-modified revision to match, or 0 for put-if-absent
     * @return PutResult
     */
    public PutResult put(ByteString key, ByteString value, long leaseId, long modRev) {
        return put(key, value, leaseId, modRevCompare(key, modRev));
    }
    
    /**
     * Multi-purpose put or delete. If successful returns modRevision
     * of updated keyValue or 0 if deleted
     *
     * @param key
     * @param value new value to put, or null to delete
     * @param lease leaseId to associate value with if successful, or 0L for no lease
     * @param conditions conditions which must all match to proceed, null or empty for unconditional
     * @return -1 if condition failed, else modRevision of updated keyValue
     */
    public long putNoGet(ByteString key, ByteString value, long lease,
            CompareOrBuilder... conditions) {

        ListenableFuture<TxnResponse> tf = doPut(key, value, false, lease, conditions);
        //TODO -- async option
        TxnResponse tr = waitFor(tf, TIMEOUT_MS);
        if(!tr.getSucceeded()) return -1L;
        else if(value != null) {
            KeyValue kv = tr.getResponses(1).getResponseRange().getKvs(0);
            offerUpdate(kv, false);
            return kv.getModRevision();
        } else {
            offerDelete(key, tr.getHeader().getRevision());
            return 0L; //TODO TBD return modRevision or 0 in this case?
        }
    }
    
   /**
    * @param key
    * @param value new value to put, or null to delete
    * @param modRev last-modified revision to match, or 0 for put-if-absent
    * @return -1 if condition failed, else modRevision of updated keyValue
    */
    public long putNoGet(ByteString key, ByteString value, long modRev) {
        return putNoGet(key, value, 0L, modRevCompare(key, modRev));
    }
    
    /**
     * @param key
     * @param value new value to put, or null to delete
     * @param leaseId leaseId to associate value with if successful, or 0L for no lease
     * @param modRev last-modified rev to match, or 0 for put-if-absent
     * @return -1 if condition failed, else modRevision of updated keyValue
     */
    public long putNoGet(ByteString key, ByteString value, long leaseId, long modRev) {
        return putNoGet(key, value, leaseId, modRevCompare(key, modRev));
    }
    
    protected static Compare.Builder modRevCompare(ByteString key, long modRev) {
        return Compare.newBuilder().setKey(key).setTarget(CompareTarget.MOD)
                .setResult(CompareResult.EQUAL).setModRevision(modRev);
    }
    
    protected ListenableFuture<TxnResponse> doPut(ByteString key, ByteString value,
            boolean getOnFail, long lease, CompareOrBuilder... conditions) {
        TxnRequest.Builder tb = TxnRequest.newBuilder();
        if(conditions != null && conditions.length > 0) {
            for(CompareOrBuilder comp : conditions) {
               if(comp instanceof Compare) tb.addCompare((Compare)comp);
               else tb.addCompare((Compare.Builder)comp);
            }
        } else getOnFail = false;
        RequestOp.Builder bld = RequestOp.newBuilder();
        RequestOp getOp = getOnFail || value != null ? getReq(bld, key) : null;
        if(value != null) tb.addSuccess(putReq(bld, key, value, lease)).addSuccess(getOp);
        else tb.addSuccess(deleteReq(bld, key));
        if(getOnFail) tb.addFailure(getOp);
        return kvClient.txn(tb.build());
    }
    private static RequestOp getReq(RequestOp.Builder bld, ByteString key) {
        return bld.setRequestRange(RangeRequest.newBuilder().setKey(key)).build();
    }
    private static RequestOp putReq(RequestOp.Builder bld, ByteString key,
            ByteString value, long lease) {
        return bld.setRequestPut(PutRequest.newBuilder().setKey(key)
                .setValue(value).setLease(lease)).build();
    }
    private static RequestOp deleteReq(RequestOp.Builder bld, ByteString key) {
        return bld.setRequestDeleteRange(DeleteRangeRequest.newBuilder().setKey(key)).build();
    }
    
    /**
     * Unconditional delete
     * 
     * @param key
     * @return true if entry was deleted, false if already absent
     */
    public boolean delete(ByteString key) {
        ListenableFuture<DeleteRangeResponse> df = kvClient.delete(key).async();
        //TODO -- async version
        DeleteRangeResponse drr = waitFor(df, TIMEOUT_MS);
        offerDelete(key, drr.getHeader().getRevision());
        return drr.getDeleted() > 0;
    }
    
    /**
     * Conditional delete
     * 
     * @param key
     * @param modRev
     * @return true if entry was deleted, false if already absent
     */
    public boolean delete(ByteString key, long modRev) {
        return putNoGet(key, null, modRev) != -1L;
    }
    
    public Set<ByteString> keySet() {
        return entries.keySet();
    }
    
    /**
     * {@link Iterator#remove()} not supported on returned iterators
     * 
     * @return an {@link Iterator} over the {@link KeyValues} of this cache
     */
    public Iterator<KeyValue> iterator() {
        // filtering iterator is unmodifiable
        return Iterators.filter(entries.values().iterator(), kv -> !isDeleted(kv));
    }
    
    
    /**
     * Iterator whose contents is guaranteed to be sequentially consistent
     * with remote updates to the cached range.
     * 
     * @return an {@link Iterator} over the {@link KeyValues} of this cache
     */
    public Iterator<KeyValue> strongIterator() {
        entries.get(fromKey); // memory barrier prior to reading seenUpToRev
        long seenUpTo = seenUpToRev;
        
        if(seenUpTo == 0L) {
            ListenableFuture<Boolean> startFut;
            synchronized(this) {
                startFut = startFuture;
            }
            if(startFut == null) {
                // cache has not yet been started
                return kvClient.get(fromKey).rangeEnd(toKey)
                        .timeout(120_000L).sync().getKvsList().iterator();
            } else try {
                startFut.get(2L, TimeUnit.MINUTES);
                // now started
                seenUpTo = seenUpToRev;
            } catch(TimeoutException te) {
                throw Status.DEADLINE_EXCEEDED.asRuntimeException();
            } catch(ExecutionException e) {
                throw Status.UNKNOWN.withCause(e).asRuntimeException();
            } catch(InterruptedException|CancellationException e) {
                throw Status.CANCELLED.withCause(e).asRuntimeException();
            }
        }
        
        /* 
         * This logic is similar to that in fullRefreshCache(), but
         * it includes an optimistic initial comparison of counts
         * to identify cases where no deletions have been missed and
         * thus a retrieval of all the keys isn't required.
         */

        RangeRequest.Builder rangeReqBld = RangeRequest.newBuilder()
                .setKey(fromKey).setRangeEnd(toKey);
        RangeRequest curCountReq = rangeReqBld.setCountOnly(true)
                .setMaxCreateRevision(seenUpTo).build();
        RangeRequest seenCountReq = rangeReqBld.clearMaxCreateRevision()
                .setRevision(seenUpTo).build(); // (countOnly still true here)
        RangeRequest newModsReq = rangeReqBld.clearRevision().clearCountOnly()
                .setMinModRevision(seenUpTo+1).build();

        // first, attempt to get:
        //  0- kvs modified since seenUpTo
        //  1- current count excluding those created since seenUpTo
        //  2- count at revision seenUpTo (this could potentially 
        //     fail with compaction error, see below)
        ListenableFuture<TxnResponse> txn = kvClient.batch()
                .get(newModsReq).get(curCountReq).get(seenCountReq).async();
        TxnResponse txnResp;
        try {
            txnResp = waitFor(txn, 8000L);
        } catch(RuntimeException e) {
            Code code = Status.fromThrowable(e).getCode();
            if(code != Code.OUT_OF_RANGE) throw e;
            // if (2) above fails due to compaction, also retrieve all current keys
            RangeRequest otherKeysReq = rangeReqBld.clearMinModRevision()
                    .setMaxModRevision(seenUpTo).setKeysOnly(true).build();
            txnResp = waitFor(kvClient.batch().get(newModsReq).get(otherKeysReq)
                    .async(), 60_000L); // longer timeout
        }

        long revNow = txnResp.getHeader().getRevision();
        if(revNow > seenUpToRev) {
            RangeResponse newModKvs = txnResp.getResponses(0).getResponseRange();
            List<KeyValue> otherKeys;
            if(txnResp.getResponsesCount() == 2) {
                // this means we must have taken the compacted exception path above
                otherKeys = txnResp.getResponses(1).getResponseRange().getKvsList();
            }
            else if(txnResp.getResponses(1).getResponseRange().getCount() <  // <- latest count
                    txnResp.getResponses(2).getResponseRange().getCount()) { // <- count at seenUpTo
                // if counts don't match, there must have been deletions since seenUpTo,
                // so additionally retrieve all current keys
                RangeRequest otherKeysReq = rangeReqBld.clearMinModRevision()
                        .setMaxModRevision(seenUpTo).setKeysOnly(true).build();
                otherKeys = waitFor(kvClient.get(otherKeysReq), 60_000L).getKvsList(); // longer timeout
            }
            else otherKeys = null;

            boolean newKvs = newModKvs.getKvsCount() > 0;
            if(otherKeys != null) { // if this is true, there *might* be deletions to process
                if(otherKeys.isEmpty() && !newKvs) return Collections.emptyIterator();
                // bring cache up to date with recently deleted kvs
                Set<ByteString> keys = Stream.concat(otherKeys.stream(), newModKvs.getKvsList().stream())
                        .map(kv -> kv.getKey()).collect(Collectors.toSet());
                entries.values().stream().filter(kv -> kv.getModRevision() < revNow && !keys.contains(kv.getKey()))
                .forEach(kv -> offerDelete(kv.getKey(), revNow));
            }

            // bring cache up to date with recently modified kvs
            if(newKvs) newModKvs.getKvsList().forEach(kv -> offerUpdate(kv, false));

            if(revNow > seenUpToRev) listenerExecutor.execute(() -> revisionUpdate(revNow));
        }
        return iterator();
    }
    
    
    @Override
    public synchronized void close() {
        if(closed) return;
        if(startFuture != null) {
            if(watch != null) watch.close();
            else startFuture.addListener(() -> {
                if(watch != null) watch.close();
            }, MoreExecutors.directExecutor());
        }
        closed = true;
    }
    
    public boolean isClosed() {
        return closed;
    }
    
}
