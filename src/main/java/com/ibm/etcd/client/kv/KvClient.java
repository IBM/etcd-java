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
package com.ibm.etcd.client.kv;

import java.io.Closeable;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Executor;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.ByteString;
import com.ibm.etcd.client.Condition;
import com.ibm.etcd.client.watch.RevisionCompactedException;
import com.ibm.etcd.api.DeleteRangeRequest;
import com.ibm.etcd.api.DeleteRangeRequestOrBuilder;
import com.ibm.etcd.api.DeleteRangeResponse;
import com.ibm.etcd.api.PutRequest;
import com.ibm.etcd.api.PutRequestOrBuilder;
import com.ibm.etcd.api.PutResponse;
import com.ibm.etcd.api.RangeRequest;
import com.ibm.etcd.api.RangeRequest.SortOrder;
import com.ibm.etcd.api.RangeRequest.SortTarget;
import com.ibm.etcd.api.RangeRequestOrBuilder;
import com.ibm.etcd.api.RangeResponse;
import com.ibm.etcd.api.TxnRequest;
import com.ibm.etcd.api.TxnRequestOrBuilder;
import com.ibm.etcd.api.TxnResponse;
import com.ibm.etcd.api.WatchCreateRequest;
import com.ibm.etcd.api.WatchCreateRequest.FilterType;

import io.grpc.Deadline;
import io.grpc.stub.StreamObserver;

/**
 * KV operations
 */
public interface KvClient {
    
    /**
     * Used to get, watch or delete <b>all</b> of the keys (use with caution!)
     */
    public static final ByteString ALL_KEYS = ByteString.copyFromUtf8("ALL_KEYS");
    
    interface FluentRequest<FR extends FluentRequest<FR,ReqT,RespT>,ReqT,RespT> {
        ListenableFuture<RespT> async();
        ListenableFuture<RespT> async(Executor executor);
        RespT sync();
        /**
         * timeout is <b>per attempt</b>
         */
        FR timeout(long millisecs);
        /**
         * deadline is absolute for entire request
         */
        FR deadline(Deadline deadline);
        FR backoffRetry();
        FR backoffRetry(Condition precondition);
        ReqT asRequest();
    }
    
    interface FluentRangeRequest extends FluentRequest<FluentRangeRequest,RangeRequest,RangeResponse> {
        FluentRangeRequest rangeEnd(ByteString key);
        FluentRangeRequest asPrefix();
        FluentRangeRequest andHigher();
        FluentRangeRequest limit(long limit);
        FluentRangeRequest revision(long rev);
        FluentRangeRequest sorted(SortTarget target, SortOrder order);
        FluentRangeRequest serializable();
        FluentRangeRequest serializable(boolean serializable);
        FluentRangeRequest keysOnly();
        FluentRangeRequest countOnly();
        FluentRangeRequest minModRevision(long rev);
        FluentRangeRequest maxModRevision(long rev);
        FluentRangeRequest minCreateRevision(long rev);
        FluentRangeRequest maxCreateRevision(long rev);
    }
    
    interface FluentDeleteRequest extends FluentRequest<FluentDeleteRequest,DeleteRangeRequest,DeleteRangeResponse> {
        FluentDeleteRequest rangeEnd(ByteString key);
        FluentDeleteRequest asPrefix();
        FluentDeleteRequest andHigher();
        FluentDeleteRequest prevKv();
    }
    
    interface FluentPutRequest extends FluentRequest<FluentPutRequest,PutRequest,PutResponse> {
        FluentPutRequest prevKv();
    }
    
    interface FluentTxnRequest extends FluentRequest<FluentTxnRequest,TxnRequest,TxnResponse> {
        FluentCmpTarget cmpEqual(ByteString key);
        FluentCmpTarget cmpNotEqual(ByteString key);
        FluentCmpTarget cmpLess(ByteString key);
        FluentCmpTarget cmpGreater(ByteString key);
        
        FluentTxnRequest exists(ByteString key);
        FluentTxnRequest notExists(ByteString key);
        default FluentTxnRequest and() { return this; }
        
        FluentTxnSuccOps then();
    }
    
    interface FluentCmpTarget {
        FluentTxnRequest version(long version);
        FluentTxnRequest mod(long rev);
        FluentTxnRequest create(long rev);
        FluentTxnRequest value(ByteString value);
        /**
         * Supported in versions &gt;= 3.3 only
         */
        FluentTxnRequest lease(long leaseId);
        
        /**
         * Supported in versions &gt;= 3.3 only
         */
        FluentCmpTarget allInRange(ByteString key);
        /**
         * Supported in versions &gt;= 3.3 only
         */
        FluentCmpTarget allWithPrefix();
        /**
         * Supported in versions &gt;= 3.3 only
         */
        FluentCmpTarget andAllHigher();
    }
    
    @SuppressWarnings("unchecked")
    interface FluentTxnOps<FTO extends FluentTxnOps<FTO>>
        extends FluentRequest<FluentTxnOps<FTO>,TxnRequest,TxnResponse> {
        //TODO maybe deeper options here
        FTO put(PutRequestOrBuilder putReq);
        FTO get(RangeRequestOrBuilder getReq);
        FTO delete(DeleteRangeRequestOrBuilder deleteReq);
        /**
         * Supported in versions &gt;= 3.3 only
         */
        FTO subTxn(TxnRequestOrBuilder txnReq);
        default FTO and() { return (FTO)this; }
        default FTO noop() { return (FTO)this; }
    }
    
    interface FluentTxnSuccOps extends FluentTxnOps<FluentTxnSuccOps> {
        FluentTxnOps<?> elseDo();
    }
    
    //TODO
    enum RetryStrategy { BASIC, BACKOFF }

    /**
     * 
     * @param request
     * @return future for {@link RangeResponse}
     */
    ListenableFuture<RangeResponse> get(RangeRequest request);
    
    
    /**
     * 
     * @param key key to get or {@link #ALL_KEYS} for the entire keyspace
     */
    FluentRangeRequest get(ByteString key);
    
    /**
     * 
     * @param txn
     * @return future for {@link TxnResponse}
     */
    ListenableFuture<TxnResponse> txn(TxnRequest txn);
    
    /**
     * 
     * @param txn
     * @param timeoutMillis
     * @return {@link TxnResponse}
     */
    TxnResponse txnSync(TxnRequest txn, long timeoutMillis);
    
    /**
     * Start a fluent transaction request
     */
    FluentTxnRequest txnIf();
    
    /**
     * Start a fluent batch transaction request
     */
    FluentTxnOps<?> batch();
    
    
    /**
     * 
     * @param request
     * @return future for {@link PutResponse}
     */
    ListenableFuture<PutResponse> put(PutRequest request);
    
    //TODO maybe combine following two
    /**
     * Put a key/value with no associated lease. If the key already
     * exists, its value will be updated and any lease association
     * will be cleared.
     * 
     * @param key
     * @param value
     */
    FluentPutRequest put(ByteString key, ByteString value);
    
    /**
     * Put a key/value associated with a lease.
     * 
     * @param key
     * @param value
     * @param leaseId
     */
    FluentPutRequest put(ByteString key, ByteString value, long leaseId);
    
    /**
     * Associate an existing key/value with a lease.
     * 
     * @param key
     * @param leaseId
     */
    FluentPutRequest setLease(ByteString key, long leaseId);
    
    /**
     * Put a key/value without affecting its lease association if
     * the key already exists.
     * 
     * @param key
     * @param value
     */
    FluentPutRequest setValue(ByteString key, ByteString value);
    
    
    /**
     * 
     * @param request
     * @return future for {@link DeleteRangeResponse}
     */
    ListenableFuture<DeleteRangeResponse> delete(DeleteRangeRequest request);
    
    /**
     * Start a fluent delete request
     * 
     * @param key key to delete or {@link #ALL_KEYS} to delete <b>everything</b>
     */
    FluentDeleteRequest delete(ByteString key);
    
    
    interface FluentWatchRequest {
        FluentWatchRequest filters(List<FilterType> filters);
        FluentWatchRequest filters(FilterType... filters);
        FluentWatchRequest prevKv();
        FluentWatchRequest rangeEnd(ByteString key);
        FluentWatchRequest asPrefix();
        FluentWatchRequest andHigher();
        FluentWatchRequest progressNotify();
        FluentWatchRequest startRevision(long rev);
        FluentWatchRequest executor(Executor executor);
        
        /**
         * Start an asynchronous listener-based watch
         */
        Watch start(StreamObserver<WatchUpdate> updateObserver);
        /**
         * Start a synchronous iterator-based watch
         */
        WatchIterator start();
    }
    
    /**
     * Call {@link #close()} at any time to cancel the watch. The future will complete
     * with TRUE when the watch is established, FALSE if it was cancelled prior
     * to being established, or throw an exception if the watch creation failed
     * (which might be {@link RevisionCompactedException}).
     */
    public interface Watch extends Closeable, ListenableFuture<Boolean> {
        @Override
        public void close(); // doesn't throw
    }
    
    /**
     * Call {@link #close()} at any time to cancel the watch. Failure of the watch will
     * result in {@link #next()} throwing the corresponding exception after first
     * returning any pending updates. If the watch completes normally then either
     * {@link #hasNext()} will return false, or {@link #next()} will return a special
     * {@link WatchUpdate} with no events and a {@code null} response header.
     * <p>
     * Note that like {@link Iterator}s in general, this is not threadsafe.
     */
    public interface WatchIterator extends Closeable, Iterator<WatchUpdate> {
        @Override
        public void close(); // doesn't throw
    }
    
    /**
     * Watch watches on a key or prefix. The watched updates will be notified via onWatch.
     * Updates may or may not include a list of events - if the provided list is null the
     * update is communicating a progression in the global revision of the store. A first
     * update is guaranteed to be triggered (with or without events) immediately upon
     * successful establishment of the watch.
     * If the watch is slow or the required rev is compacted, the watch request
     * might be cancelled from the server-side and the onError observer will be called
     * with a {@link RevisionCompactedException}.
     *
     * @param request the watch option
     * @param updates watch update stream
     * @return Watch watch reference
     */
      Watch watch(WatchCreateRequest request, StreamObserver<WatchUpdate> updates);
      
      /**
       * Start a fluent watch request
       * 
       * @see #watch(WatchCreateRequest, StreamObserver)
       * @param key key to watch or {@link #ALL_KEYS} to watch the entire keyspace
       */
      FluentWatchRequest watch(ByteString key);
    
}
