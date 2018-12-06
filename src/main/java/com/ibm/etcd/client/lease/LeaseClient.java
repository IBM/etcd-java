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

import java.util.concurrent.Executor;

import com.google.common.util.concurrent.ListenableFuture;
import com.ibm.etcd.client.FluentRequest;
import com.ibm.etcd.client.lease.PersistentLease.LeaseState;
import com.ibm.etcd.api.LeaseGrantRequest;
import com.ibm.etcd.api.LeaseGrantResponse;
import com.ibm.etcd.api.LeaseKeepAliveResponse;
import com.ibm.etcd.api.LeaseLeasesResponse;
import com.ibm.etcd.api.LeaseRevokeResponse;
import com.ibm.etcd.api.LeaseTimeToLiveResponse;

import io.grpc.stub.StreamObserver;

/**
 * Lease operations
 */
public interface LeaseClient {
    
    interface FluentGrantRequest extends FluentRequest<FluentGrantRequest,LeaseGrantRequest,LeaseGrantResponse> {
        FluentGrantRequest leaseId(long leaseId);
    }
    
    interface FluentMaintainRequest {
        FluentMaintainRequest leaseId(long leaseId);
        FluentMaintainRequest keepAliveFreq(int frequencySecs);
        FluentMaintainRequest minTtl(int minTtlSecs);
        FluentMaintainRequest executor(Executor executor);
        FluentMaintainRequest permanent();
        PersistentLease start();
        PersistentLease start(StreamObserver<LeaseState> observer);
    }
    
    /**
     * 
     * @param leaseId
     * @param ttlSecs
     * @return future for {@link LeaseGrantResponse}
     */
    ListenableFuture<LeaseGrantResponse> create(long leaseId, long ttlSecs);
    
    /**
     * 
     * @param ttlSecs
     * @return future for {@link LeaseGrantResponse}
     */
    default ListenableFuture<LeaseGrantResponse> create(long ttlSecs) {
        return create(0L, ttlSecs);
    }
    
    /**
     * 
     * @param ttlSecs
     * @return TODO
     */
    FluentGrantRequest grant(long ttlSecs);
    
    /**
     * 
     * @param leaseId
     * @return future for {@link LeaseRevokeResponse}
     */
    ListenableFuture<LeaseRevokeResponse> revoke(long leaseId);
    
    /**
     * 
     * @param leaseId
     * @return future for {@link LeaseRevokeResponse}
     */
    ListenableFuture<LeaseRevokeResponse> revoke(long leaseId, boolean ensureWithRetries);
    
    /**
     * 
     * @param leaseId
     * @param includeKeys
     * @return future for {@link LeaseTimeToLiveResponse}
     */
    ListenableFuture<LeaseTimeToLiveResponse> ttl(long leaseId, boolean includeKeys);
    
    /**
     * 
     * @param leaseId
     * @return future for {@link LeaseTimeToLiveResponse}
     */
    default ListenableFuture<LeaseTimeToLiveResponse> ttl(long leaseId) {
        return ttl(leaseId, false);
    }
    
    /**
     * 
     * @param leaseId
     * @return future for {@link LeaseKeepAliveResponse}
     */
    ListenableFuture<LeaseKeepAliveResponse> keepAliveOnce(long leaseId);
    
    /**
     * List all existing leases
     * <p>
     * Supported in versions &gt;= 3.3 only
     * 
     * @return future for {@link LeaseLeasesResponse}
     */
    ListenableFuture<LeaseLeasesResponse> list();
    
    /**
     * Maintain a "persistent" lease. This will keep a lease alive indefinitely,
     * creating or recreating it if it doesn't exist. It can be cancelled/revoked
     * by closing the returned {@link PersistentLease} object.
     * <p>
     * Specifying a lease id is optional. If not provided it will be server-assigned.
     * The return {@link PersistentLease} is also a future, which will be completed
     * with the lease id when it is granted/confirmed.
     * <p>
     * <b>NOTE</b> sole "ownership" of the lease is assumed - i.e. the lease should
     * not be revoked elsewhere.
     * 
     */
    FluentMaintainRequest maintain();
}
