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

import static java.util.concurrent.TimeUnit.NANOSECONDS;

import java.util.concurrent.Executor;
import java.util.concurrent.TimeoutException;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.ByteString;
import com.ibm.etcd.api.LockGrpc;
import com.ibm.etcd.api.LockRequest;
import com.ibm.etcd.api.LockResponse;
import com.ibm.etcd.api.UnlockRequest;
import com.ibm.etcd.api.UnlockResponse;
import com.ibm.etcd.client.EtcdClient;
import com.ibm.etcd.client.FluentRequest.AbstractFluentRequest;
import com.ibm.etcd.client.GrpcClient;
import com.ibm.etcd.client.lease.PersistentLease;

import io.grpc.MethodDescriptor;
import io.grpc.Status;

public final class EtcdLockClient implements LockClient {
    
    // avoid volatile read on every invocation
    private static final MethodDescriptor<LockRequest,LockResponse> METHOD_LOCK =
            LockGrpc.getLockMethod();
    private static final MethodDescriptor<UnlockRequest,UnlockResponse> METHOD_UNLOCK =
            LockGrpc.getUnlockMethod();

    protected final EtcdClient etcdClient;
    protected final GrpcClient grpcClient;
    
    public EtcdLockClient(GrpcClient grpcClient, EtcdClient etcdClient) {
        this.grpcClient = grpcClient;
        this.etcdClient = etcdClient;
    }
    
    final class EtcdLockRequest extends AbstractFluentRequest<FluentLockRequest,
        LockRequest,LockResponse,LockRequest.Builder> implements FluentLockRequest {
        PersistentLease lease;
        
        EtcdLockRequest(ByteString name) {
            super(grpcClient, LockRequest.newBuilder().setName(name));
        }
        @Override
        protected MethodDescriptor<LockRequest, LockResponse> getMethod() {
            return METHOD_LOCK;
        }
        @Override
        protected boolean idempotent() {
            return true;
        }
        @Override
        public FluentLockRequest withLease(long leaseId) {
            builder.setLease(leaseId);
            lease = null;
            return this;
        }
        @Override
        public FluentLockRequest withLease(PersistentLease lease) {
            this.lease = lease;
            return this;
        }

        @Override
        public final ListenableFuture<LockResponse> async(Executor executor) {
            if(lease == null) {
                if(builder.getLease() != 0L) return super.async(executor);
                else lease = etcdClient.getSessionLease();
            }
            long plId = lease.getLeaseId();
            if(plId != 0L) {
                builder.setLease(plId);
                return super.async(executor);
            }
            ListenableFuture<Long> fut;
            if(deadline == null) fut = lease;
            else {
                long remainingNanos = deadline.timeRemaining(NANOSECONDS);
                fut = Futures.catching(Futures.withTimeout(lease,
                        remainingNanos, NANOSECONDS, grpcClient.getInternalExecutor()),
                        TimeoutException.class, te -> {
                            throw Status.DEADLINE_EXCEEDED.withCause(te)
                            .withDescription(String.format("deadline exceeded after %dns",
                                    remainingNanos)).asRuntimeException();
                        }, MoreExecutors.directExecutor());
            }
            return Futures.transformAsync(fut, id -> {
                builder.setLease(id);
                return super.async(executor);
            }, executor);
        }
    }
    
    @Override
    public FluentLockRequest lock(ByteString name) {
        return new EtcdLockRequest(name);
    }
    
    final class EtcdUnlockRequest extends AbstractFluentRequest<FluentUnlockRequest,
        UnlockRequest,UnlockResponse,UnlockRequest.Builder> implements FluentUnlockRequest {

        EtcdUnlockRequest(ByteString key) {
            super(grpcClient, UnlockRequest.newBuilder().setKey(key));
        }
        @Override
        protected MethodDescriptor<UnlockRequest, UnlockResponse> getMethod() {
            return METHOD_UNLOCK;
        }
        @Override
        protected boolean idempotent() {
            return true;
        }
    }

    @Override
    public FluentUnlockRequest unlock(ByteString key) {
        return new EtcdUnlockRequest(key);
    }
}
