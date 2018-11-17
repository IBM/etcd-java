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

import static com.ibm.etcd.client.GrpcClient.waitFor;
import static com.ibm.etcd.client.KeyUtils.ZERO_BYTE;

import java.util.List;
import java.util.concurrent.Executor;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.ByteString;
import com.ibm.etcd.api.Compare;
import com.ibm.etcd.api.Compare.CompareResult;
import com.ibm.etcd.api.Compare.CompareTarget;
import com.ibm.etcd.api.DeleteRangeRequest;
import com.ibm.etcd.api.DeleteRangeRequestOrBuilder;
import com.ibm.etcd.api.DeleteRangeResponse;
import com.ibm.etcd.api.KVGrpc;
import com.ibm.etcd.api.PutRequest;
import com.ibm.etcd.api.PutRequestOrBuilder;
import com.ibm.etcd.api.PutResponse;
import com.ibm.etcd.api.RangeRequest;
import com.ibm.etcd.api.RangeRequest.SortOrder;
import com.ibm.etcd.api.RangeRequest.SortTarget;
import com.ibm.etcd.api.RangeRequestOrBuilder;
import com.ibm.etcd.api.RangeResponse;
import com.ibm.etcd.api.RequestOp;
import com.ibm.etcd.api.RequestOp.RequestCase;
import com.ibm.etcd.api.TxnRequest;
import com.ibm.etcd.api.TxnRequestOrBuilder;
import com.ibm.etcd.api.TxnResponse;
import com.ibm.etcd.api.WatchCreateRequest;
import com.ibm.etcd.api.WatchCreateRequest.FilterType;
import com.ibm.etcd.client.Condition;
import com.ibm.etcd.client.FluentRequest.AbstractFluentRequest;
import com.ibm.etcd.client.GrpcClient;
import com.ibm.etcd.client.KeyUtils;
import com.ibm.etcd.client.watch.EtcdWatchClient;

import io.grpc.Deadline;
import io.grpc.MethodDescriptor;
import io.grpc.stub.StreamObserver;

public class EtcdKvClient implements KvClient {
    
    // avoid volatile read on every invocation
    private static final MethodDescriptor<RangeRequest,RangeResponse> METHOD_RANGE =
            KVGrpc.getRangeMethod();
    private static final MethodDescriptor<TxnRequest,TxnResponse> METHOD_TXN =
            KVGrpc.getTxnMethod();
    private static final MethodDescriptor<PutRequest,PutResponse> METHOD_PUT =
            KVGrpc.getPutMethod();
    private static final MethodDescriptor<DeleteRangeRequest,DeleteRangeResponse> METHOD_DELETE_RANGE =
            KVGrpc.getDeleteRangeMethod();
    
    protected final GrpcClient client;
    
    public EtcdKvClient(GrpcClient client) {
        this.client = client;
    }
    
    @Override
    public ListenableFuture<RangeResponse> get(RangeRequest request) {
        return client.call(METHOD_RANGE, request, true);
    }
    
    @Override
    public FluentRangeRequest get(ByteString key) {
        return new EtcdRangeRequest(key);
    }

    //TODO reinstate txn idempotence determination
    private static Predicate<TxnRequest> IDEMPOTENT_TXN = txn ->
        Iterables.all(Iterables.concat(txn.getSuccessList(),txn.getFailureList()), op ->
            op.getRequestCase() == RequestCase.REQUEST_RANGE);
    
    @Override
    public ListenableFuture<TxnResponse> txn(TxnRequest txn) {
        return client.call(METHOD_TXN, txn, false); //IDEMPOTENT_TXN);
    }
    
    @Override
    public FluentTxnRequest txnIf() {
        return new EtcdTxnRequest();
    }
    
    @Override
    public FluentTxnOps<?> batch() {
        return txnIf().then();
    }

    @Override
    public TxnResponse txnSync(TxnRequest txn, long timeoutMillis) {
        return waitFor(ex -> client.call(METHOD_TXN, txn, false, timeoutMillis, ex));
    }
    
    @Override
    public ListenableFuture<PutResponse> put(PutRequest request) {
        return client.call(METHOD_PUT, request, false);
    }
    
    @Override
    public FluentPutRequest put(ByteString key, ByteString value) {
        return new EtcdPutRequest(key, value, false);
    }
    
    @Override
    public FluentPutRequest put(ByteString key, ByteString value, long leaseId) {
        return new EtcdPutRequest(key, value, leaseId);
    }
    
    @Override
    public FluentPutRequest setLease(ByteString key, long leaseId) {
        return new EtcdPutRequest(key, leaseId);
    }
    
    @Override
    public FluentPutRequest setValue(ByteString key, ByteString value) {
        return new EtcdPutRequest(key, value, true);
    }

    @Override
    public ListenableFuture<DeleteRangeResponse> delete(DeleteRangeRequest request) {
        return client.call(METHOD_DELETE_RANGE, request, false);
    }
    
    @Override
    public FluentDeleteRequest delete(ByteString key) {
        return new EtcdDeleteRequest(key);
    }

    class EtcdRangeRequest extends AbstractFluentRequest<FluentRangeRequest,RangeRequest,
        RangeResponse,RangeRequest.Builder> implements FluentRangeRequest {
        
        EtcdRangeRequest(ByteString key) {
            super(EtcdKvClient.this.client, RangeRequest.newBuilder());
            if(key != ALL_KEYS) builder.setKey(key);
            else builder.setKey(ZERO_BYTE).setRangeEnd(ZERO_BYTE);
        }
        @Override
        protected MethodDescriptor<RangeRequest, RangeResponse> getMethod() {
            return METHOD_RANGE;
        }
        @Override
        protected boolean idempotent() {
            return true;
        }
        @Override
        public FluentRangeRequest rangeEnd(ByteString key) {
            builder.setRangeEnd(key);
            return this;
        }
        @Override
        public FluentRangeRequest asPrefix() {
            builder.setRangeEnd(KeyUtils.plusOne(builder.getKey()));
            return this;
        }
        @Override
        public FluentRangeRequest andHigher() {
            builder.setRangeEnd(ZERO_BYTE);
            return this;
        }
        @Override
        public FluentRangeRequest limit(long limit) {
            builder.setLimit(limit);
            return this;
        }
        @Override
        public FluentRangeRequest revision(long rev) {
            builder.setRevision(rev);
            return this;
        }
        @Override
        public FluentRangeRequest sorted(SortTarget target, SortOrder order) {
            builder.setSortTarget(target).setSortOrder(order);
            return this;
        }
        @Override
        public FluentRangeRequest serializable(boolean serializable) {
            builder.setSerializable(serializable);
            return this;
        }
        @Override
        public FluentRangeRequest serializable() {
            builder.setSerializable(true);
            return this;
        }
        @Override
        public FluentRangeRequest keysOnly() {
            builder.setKeysOnly(true);
            return this;
        }
        @Override
        public FluentRangeRequest countOnly() {
            builder.setCountOnly(true);
            return this;
        }
        @Override
        public FluentRangeRequest minModRevision(long rev) {
            builder.setMinModRevision(rev);
            return this;
        }
        @Override
        public FluentRangeRequest maxModRevision(long rev) {
            builder.setMaxModRevision(rev);
            return this;
        }
        @Override
        public FluentRangeRequest minCreateRevision(long rev) {
            builder.setMinCreateRevision(rev);
            return this;
        }
        @Override
        public FluentRangeRequest maxCreateRevision(long rev) {
            builder.setMaxCreateRevision(rev);
            return this;
        }
    }
    
    class EtcdDeleteRequest extends AbstractFluentRequest<FluentDeleteRequest,DeleteRangeRequest,
        DeleteRangeResponse,DeleteRangeRequest.Builder> implements FluentDeleteRequest {
        
        EtcdDeleteRequest(ByteString key) {
            super(EtcdKvClient.this.client, DeleteRangeRequest.newBuilder());
            if(key != ALL_KEYS) builder.setKey(key);
            else builder.setKey(ZERO_BYTE).setRangeEnd(ZERO_BYTE);
        }
        @Override
        protected MethodDescriptor<DeleteRangeRequest, DeleteRangeResponse> getMethod() {
            return METHOD_DELETE_RANGE;
        }
        @Override
        protected boolean idempotent() {
            return false;
        }
        @Override
        public FluentDeleteRequest rangeEnd(ByteString key) {
            builder.setRangeEnd(key);
            return this;
        }
        @Override
        public FluentDeleteRequest asPrefix() {
            builder.setRangeEnd(KeyUtils.plusOne(builder.getKey()));
            return this;
        }
        @Override
        public FluentDeleteRequest andHigher() {
            builder.setRangeEnd(ZERO_BYTE);
            return this;
        }
        @Override
        public FluentDeleteRequest prevKv() {
            builder.setPrevKv(true);
            return this;
        }
    }
    
    class EtcdPutRequest extends AbstractFluentRequest<FluentPutRequest,PutRequest,
        PutResponse,PutRequest.Builder> implements FluentPutRequest {
        EtcdPutRequest() {
            super(EtcdKvClient.this.client, PutRequest.newBuilder());
        }
        EtcdPutRequest(ByteString key, ByteString value, boolean ignoreLease) {
            this();
            builder.setKey(key).setValue(value).setIgnoreLease(ignoreLease);
        }
        
        EtcdPutRequest(ByteString key, ByteString value, long leaseId) {
            this();
            builder.setKey(key).setValue(value).setLease(leaseId);
        }
        
        EtcdPutRequest(ByteString key, long lease) {
            this();
            builder.setKey(key).setLease(lease).setIgnoreValue(true);
        }
        @Override
        protected MethodDescriptor<PutRequest, PutResponse> getMethod() {
            return METHOD_PUT;
        }
        @Override
        protected boolean idempotent() {
            return false;
        }
        @Override
        public FluentPutRequest prevKv() {
            builder.setPrevKv(true);
            return this;
        }
    }
    
    class EtcdTxnRequest extends AbstractFluentRequest<FluentTxnRequest,TxnRequest,
        TxnResponse,TxnRequest.Builder> implements FluentTxnRequest {
        
        final Compare.Builder cmpBld = Compare.newBuilder(); //reused
        final FluentCmpTarget CMP_TARGET = new EtcdCmpTarget();
        FluentTxnSuccOps TXN_OPS = null; // lazy instantiate
        boolean idempotent = true; // set to false when any non-idempotent ops are added
        
        public EtcdTxnRequest() {
            super(EtcdKvClient.this.client, TxnRequest.newBuilder());
        }
        @Override
        protected MethodDescriptor<TxnRequest, TxnResponse> getMethod() {
            return METHOD_TXN;
        }
        @Override
        protected boolean idempotent() {
            return idempotent;
        }        
        @Override
        public FluentCmpTarget cmpEqual(ByteString key) {
            return cmp(key, CompareResult.EQUAL);
        }
        @Override
        public FluentCmpTarget cmpNotEqual(ByteString key) {
            return cmp(key, CompareResult.NOT_EQUAL);
        }
        @Override
        public FluentCmpTarget cmpLess(ByteString key) {
            return cmp(key, CompareResult.LESS);
        }
        @Override
        public FluentCmpTarget cmpGreater(ByteString key) {
            return cmp(key, CompareResult.GREATER);
        }
        @Override
        public FluentTxnRequest exists(ByteString key) {
            return cmpNotEqual(key).version(0L);
        }
        @Override
        public FluentTxnRequest notExists(ByteString key) {
            return cmpEqual(key).version(0L);
        }
        private FluentCmpTarget cmp(ByteString key, CompareResult cr) {
            cmpBld.setKey(key).setResult(cr);
            return CMP_TARGET;
        }
        
        @Override
        public FluentTxnSuccOps then() {
            return TXN_OPS != null ? TXN_OPS
                    : (TXN_OPS = new EtcdTxnOps());
        }
        
        class EtcdCmpTarget implements FluentCmpTarget {
            @Override
            public FluentCmpTarget allInRange(ByteString rangeEnd) {
                cmpBld.setRangeEnd(rangeEnd);
                return this;
            }
            @Override
            public FluentCmpTarget allWithPrefix() {
                cmpBld.setRangeEnd(KeyUtils.plusOne(cmpBld.getKey()));
                return this;
            }
            @Override
            public FluentCmpTarget andAllHigher() {
                cmpBld.setRangeEnd(ZERO_BYTE);
                return this;
            }
            @Override
            public FluentTxnRequest version(long version) {
                cmpBld.setVersion(version);
                return add(CompareTarget.VERSION);
            }
            @Override
            public FluentTxnRequest mod(long rev) {
                cmpBld.setModRevision(rev);
                return add(CompareTarget.MOD);
            }
            @Override
            public FluentTxnRequest create(long rev) {
                cmpBld.setCreateRevision(rev);
                return add(CompareTarget.CREATE);
            }
            @Override
            public FluentTxnRequest value(ByteString value) {
                cmpBld.setValue(value);
                return add(CompareTarget.VALUE);
            }
            @Override
            public FluentTxnRequest lease(long leaseId) {
                cmpBld.setLease(leaseId);
                return add(CompareTarget.LEASE);
            }
            private FluentTxnRequest add(CompareTarget ct) {
                builder.addCompare(cmpBld.setTarget(ct));
                return EtcdTxnRequest.this;
            }
        }
        
        class EtcdTxnOps implements FluentTxnSuccOps {
            private final RequestOp.Builder opBld = RequestOp.newBuilder();
            private boolean succ = true;
            
            private FluentTxnSuccOps add(RequestOp.Builder op) {
                if(succ) builder.addSuccess(op);
                else builder.addFailure(op);
                return this;
            }
            @Override
            public FluentTxnOps<?> elseDo() {
                succ = false;
                return this;
            }
            @Override
            public FluentTxnSuccOps put(PutRequestOrBuilder putReq) {
                idempotent = false;
                return add(putReq instanceof PutRequest ?
                        opBld.setRequestPut((PutRequest)putReq)
                        : opBld.setRequestPut((PutRequest.Builder)putReq));
            }
            @Override
            public FluentTxnSuccOps get(RangeRequestOrBuilder getReq) {
                return add(getReq instanceof RangeRequest ?
                        opBld.setRequestRange((RangeRequest)getReq)
                        : opBld.setRequestRange((RangeRequest.Builder)getReq));
            }
            @Override
            public FluentTxnSuccOps delete(DeleteRangeRequestOrBuilder delReq) {
                idempotent = false;
                return add(delReq instanceof DeleteRangeRequest ?
                        opBld.setRequestDeleteRange((DeleteRangeRequest)delReq)
                        : opBld.setRequestDeleteRange((DeleteRangeRequest.Builder)delReq));
            }
            @Override
            public FluentTxnSuccOps subTxn(TxnRequestOrBuilder txnReq) {
                idempotent = false; //TODO determine idempotence of sub txn
                return add(txnReq instanceof TxnRequest ?
                        opBld.setRequestTxn((TxnRequest)txnReq)
                        : opBld.setRequestTxn((TxnRequest.Builder)txnReq));
            }
            @Override
            public FluentTxnOps<FluentTxnSuccOps> backoffRetry() {
                EtcdTxnRequest.this.backoffRetry();
                return this;
            }
            @Override
            public FluentTxnOps<FluentTxnSuccOps> backoffRetry(Condition precondition) {
                EtcdTxnRequest.this.backoffRetry(precondition);
                return this;
            }
            @Override
            public FluentTxnOps<FluentTxnSuccOps> timeout(long millisecs) {
                EtcdTxnRequest.this.timeout(millisecs);
                return this;
            }
            @Override
            public FluentTxnOps<FluentTxnSuccOps> deadline(Deadline deadline) {
                EtcdTxnRequest.this.deadline(deadline);
                return this;
            }
            @Override
            public ListenableFuture<TxnResponse> async() {
                return EtcdTxnRequest.this.async();
            }
            @Override
            public ListenableFuture<TxnResponse> async(Executor executor) {
                return EtcdTxnRequest.this.async(executor);
            }
            @Override
            public TxnRequest asRequest() {
                return EtcdTxnRequest.this.asRequest();
            }
            @Override
            public TxnResponse sync() {
                return EtcdTxnRequest.this.sync();
            }
        }
    }
    
    private volatile EtcdWatchClient watchClient;
    
    private boolean closed;
    
    private EtcdWatchClient watchClient() {
        EtcdWatchClient wc = watchClient;
        if(wc == null) synchronized(this) {
            if(closed) throw new IllegalStateException("client closed");
            if((wc=watchClient) == null) {
                watchClient = wc = new EtcdWatchClient(client);
            }
        }
        return wc;
    }
    
    public void close() {
        synchronized(this) {
            if(closed) return;
            closed = true;
            if(watchClient != null) watchClient.close();
        }
    }
    
    class EtcdWatchRequest implements FluentWatchRequest {
        private final WatchCreateRequest.Builder builder = WatchCreateRequest.newBuilder();
        
        private Executor executor;

        EtcdWatchRequest(ByteString key) {
            if(key != ALL_KEYS) builder.setKey(key);
            else builder.setKey(ZERO_BYTE).setRangeEnd(ZERO_BYTE);
        }
        
        @Override
        public FluentWatchRequest filters(List<FilterType> filters) {
            builder.addAllFilters(filters);
            return this;
        }
        @Override
        public FluentWatchRequest filters(FilterType... filters) {
            for(FilterType ft : filters) builder.addFilters(ft);
            return this;
        }
        @Override
        public FluentWatchRequest prevKv() {
            builder.setPrevKv(true);
            return this;
        }
        @Override
        public FluentWatchRequest rangeEnd(ByteString key) {
            builder.setRangeEnd(key);
            return this;
        }
        @Override
        public FluentWatchRequest asPrefix() {
            builder.setRangeEnd(KeyUtils.plusOne(builder.getKey()));
            return this;
        }
        @Override
        public FluentWatchRequest andHigher() {
            builder.setRangeEnd(ZERO_BYTE);
            return this;
        }
        @Override
        public FluentWatchRequest progressNotify() {
            builder.setProgressNotify(true);
            return this;
        }
        @Override
        public FluentWatchRequest startRevision(long rev) {
            builder.setStartRevision(rev);
            return this;
        }
        @Override
        public FluentWatchRequest executor(Executor executor) {
            this.executor = executor;
            return this;
        }

        @Override
        public Watch start(StreamObserver<WatchUpdate> updateObserver) {
            return watchClient().watch(builder.build(), updateObserver, executor);
        }
        @Override
        public WatchIterator start() {
            if(executor != null) throw new IllegalArgumentException(
                    "executor provided for iterator-based watch");
            return watchClient().watch(builder.build());
        }
    }

    @Override
    public Watch watch(WatchCreateRequest request, StreamObserver<WatchUpdate> updates) {
        return watchClient().watch(request, updates);
    }
    
    @Override
    public FluentWatchRequest watch(ByteString key) {
        return new EtcdWatchRequest(key);
    }
    
}
