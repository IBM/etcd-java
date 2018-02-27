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

import java.util.concurrent.CancellationException;
import java.util.concurrent.Executor;

import com.google.common.base.Function;
import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import com.google.protobuf.ByteString;
import com.ibm.etcd.client.EtcdClient;
import com.ibm.etcd.client.FutureListener;
import com.ibm.etcd.client.ListenerObserver;
import com.ibm.etcd.client.SerializingExecutor;
import com.ibm.etcd.client.kv.KvClient;
import com.ibm.etcd.client.lease.PersistentLease;
import com.ibm.etcd.client.lease.PersistentLease.LeaseState;
import com.ibm.etcd.api.PutRequest;
import com.ibm.etcd.api.RangeRequest;
import com.ibm.etcd.api.TxnResponse;

/**
 * Etcd key-value bound to a PersistentLease. If the key already exists it's
 * value won't be changed but it will be associated with the provided lease.
 * If it doesn't already exist or is deleted by someone else, it will be
 * (re)-created with a provided default value.
 * <p>
 * Can be optionally associated with a {@link RangeCache} within whose range
 * the key lies. Doing so ensures the contents of the RangeCache will immediately
 * reflect changes in the key-value's state caused by this class. In particular,
 * the key will be removed from the cache upon lease expiry while disconnected
 * from the etcd cluster.
 * <p>
 * Closing the {@link PersistentLeaseKey} will always delete the associated
 * key-value.
 * 
 */
public class PersistentLeaseKey extends AbstractFuture<ByteString> implements AutoCloseable {

    private final EtcdClient client;
    protected final ByteString key;
    protected final ListenerObserver<LeaseState> stateObserver;
    
    private final RangeCache rangeCache; // optional
    
    private PersistentLease lease; // final post-start
    private Executor executor; // serialized, final post-start
    
    private volatile ByteString defaultValue;
    
    // these only modified in serialized context
    protected ListenableFuture<?> updateFuture;
    protected SettableFuture<Object> closeFuture; // non-null => closing or closed
    
    /**
     * 
     * @param client
     * @param lease
     * @param key
     * @param defaultValue
     * @param rangeCache optional, may be null
     */
    public PersistentLeaseKey(EtcdClient client, PersistentLease lease,
            ByteString key, ByteString defaultValue, RangeCache rangeCache) {
        this.client = client;
        //TODO if rangeCache != null, verify key lies within it's range
        this.rangeCache = rangeCache;
        this.lease = lease;
        this.key = key;
        this.defaultValue = defaultValue;
        this.stateObserver = this::leaseStateChanged;
    }
    
    protected void leaseStateChanged(boolean c, LeaseState newState, Throwable t) {
        executor.execute(() -> {
            if(newState == LeaseState.ACTIVE) putKey(lease.getLeaseId());
            else if(newState == LeaseState.EXPIRED && rangeCache != null) {
                rangeCache.offerExpiry(key);
            }
        });
    }
    
    protected boolean isActive() {
        return lease != null && lease.getState() == LeaseState.ACTIVE;
    }
    
    /**
     * Create a {@link PersistentLeaseKey} associated with the provided
     * client's session lease.
     * 
     * @param client
     * @param key
     * @param defaultValue
     * @param rangeCache
     */
    public PersistentLeaseKey(EtcdClient client,
            ByteString key, ByteString defaultValue, RangeCache rangeCache) {
        this(client, client.getSessionLease(), key, defaultValue, rangeCache);
    }
    
    public synchronized void start() {
        if(executor != null) throw new IllegalStateException("already started");
        if(closeFuture != null) throw new IllegalStateException("closed");
        //TODO TBD or have lease expose its response executor
        executor = new SerializingExecutor(client.getExecutor());
        if(lease == null) lease = client.getSessionLease();
        lease.addStateObserver(stateObserver, true);
    }
    
    /**
     * @return a future completed when the key is created and associated with the lease
     */
    public ListenableFuture<ByteString> startWithFuture() {
        start();
        return this;
    }
    
    /**
     * Sets value to use if keyvalue has to be recreated, value of key on
     * server isn't otherwise changed
     * 
     * @param value
     */
    public void setDefaultValue(ByteString value) {
        this.defaultValue = value;
    }
    
    // called only from our serialized executor context
    protected void putKey(long leaseId) {
        if(leaseId == 0L || closeFuture != null) return;
        if(updateFuture != null && !updateFuture.isDone()) {
            // if the cancellation wins then putKey will be immediately retried
            updateFuture.cancel(false);
            return;
        }
        
        // execute a transaction which either sets the lease on an existing key
        // or creates the key with the lease if it doesn't exist
        PutRequest.Builder putBld = PutRequest.newBuilder().setKey(key).setLease(leaseId);
        KvClient.FluentTxnRequest req = client.getKvClient().txnIf().exists(key)
                .backoffRetry(() -> closeFuture == null && isActive());
        ListenableFuture<? extends Object> fut;
        ListenableFuture<TxnResponse> txnFut;
        if(rangeCache == null) {
            fut = txnFut = req.then().put(putBld.setIgnoreValue(true))
                    .elseDo().put(putBld.setIgnoreValue(false).setValue(defaultValue))
                    .async();
        } else {
            RangeRequest getOp = RangeRequest.newBuilder().setKey(key).build();
            txnFut = req.then().put(putBld.setIgnoreValue(true)).get(getOp)
                    .elseDo().put(putBld.setIgnoreValue(false).setValue(defaultValue)).get(getOp)
                    .async();
            fut = Futures.transform(txnFut, (Function<TxnResponse,Object>)tr
                    -> rangeCache.offerUpdate(tr.getResponses(1).getResponseRange().getKvs(0), false));
        }
        if(!isDone()) fut = Futures.transform(fut, (Function<Object,Object>) r -> set(key));
        // this callback is to trigger an immediate retry in case the attempt was cancelled by a more
        // recent lease state change to active
        Futures.addCallback(fut, (FutureListener<Object>) (v,t) -> {
            if(t instanceof CancellationException && isActive()) putKey(leaseId);
        }, executor);
        
        updateFuture = fut;
    }
    
    @Override
    protected void interruptTask() {
        close();
    }

    /**
     * Closing deletes the key.
     */
    @Override
    public void close() {
        closeWithFuture();
    }

    /**
     * @return future completes when key is verified deleted
     */
    public ListenableFuture<?> closeWithFuture() {
        boolean notStarted = false;
        synchronized(this) {
            if(closeFuture != null) return closeFuture;
            closeFuture = SettableFuture.create();
            if(executor == null) notStarted = true;
            else {
                lease.removeStateObserver(stateObserver);
                executor.execute(() -> {
                    if(updateFuture == null || updateFuture.isDone()) deleteKey();
                    else updateFuture.addListener(this::deleteKey, executor);
                });
            }
        }
        // do these outside sync block since they may call other listeners
        setException(new CancellationException("closed"));
        if(notStarted) closeFuture.set(null);
        return closeFuture;
    }
    
    private void deleteKey() {
        client.getKvClient().delete(key)
        .backoffRetry(() -> lease.getState() != LeaseState.CLOSED).async()
        .addListener(() -> closeFuture.set(null), MoreExecutors.directExecutor());
    }
}
