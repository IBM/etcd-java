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

import java.io.Closeable;

import com.google.common.util.concurrent.ListenableFuture;

import io.grpc.stub.StreamObserver;

/**
 * A lease which is kept alive indefinitely and (re)created when necessary.
 * <p>
 * This class is also a future which completes with the lease id once the
 * lease is created or verified.
 * <p>
 * Use the {@link #close()} method to cancel/revoke the lease
 */
public interface PersistentLease extends ListenableFuture<Long>, Closeable {

    enum LeaseState {
        /**
         * Lease existence not yet established
         */
        PENDING, // -> ACTIVE or CLOSED
        /**
         * Lease active
         */
        ACTIVE, // -> ACTIVE_NO_CONN or EXPIRED or CLOSED
        /**
         * Lease should be active, but there are keep-alive connection
         * problems - it will expire if these don't recover prior to
         * the remaining TTL
         */
        ACTIVE_NO_CONN, // -> ACTIVE or EXPIRED or CLOSED
        /**
         * Lease has likely expired, will be reinstated as soon as
         * connectivity permits
         */
        EXPIRED, // -> ACTIVE or CLOSED
        /**
         * Closed
         */
        CLOSED // terminal
    }

    /**
     * @return the leaseId, or 0 if pending
     */
    long getLeaseId(); // may return 0 before initial grant
    /**
     * @return the current lease state
     * @see LeaseState
     */
    LeaseState getState();
    /**
     * @return the ttl which will be used if/when (re)creating the lease
     */
    long getPreferredTtlSecs();
    /**
     * @return the ttl of the last successful keepalive,
     *    or -1 before initial grant
     */
    long getKeepAliveTtlSecs();
    /**
     * @return the number of seconds before this lease expires,
     *    or -1 before initial grant
     */
    long getCurrentTtlSecs();  //TODO(maybe) or just get deadline

    //TODO add blocking waitForState(LeaseState) method

    /**
     * 
     * @param observer
     * @param publishInitialState if true, the {@link StreamObserver#onNext(Object)}
     *     method of the supplied observer will be called first with the <i>current</i>
     *     state regardless of when it last changed. Subsequent callbacks will correspond
     *     to state changes.
     */
    void addStateObserver(StreamObserver<LeaseState> observer, boolean publishInitialState);

    void removeStateObserver(StreamObserver<LeaseState> observer);
}