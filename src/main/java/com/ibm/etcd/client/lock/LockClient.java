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
package com.ibm.etcd.client.lock;

import com.google.protobuf.ByteString;
import com.ibm.etcd.api.LockRequest;
import com.ibm.etcd.api.LockResponse;
import com.ibm.etcd.api.UnlockRequest;
import com.ibm.etcd.api.UnlockResponse;
import com.ibm.etcd.client.FluentRequest;
import com.ibm.etcd.client.lease.PersistentLease;

/**
 * Lock operations
 */
public interface LockClient {

    interface FluentLockRequest extends FluentRequest<FluentLockRequest,LockRequest,LockResponse> {
        FluentLockRequest withLease(long leaseId);
        FluentLockRequest withLease(PersistentLease lease);
    }

    interface FluentUnlockRequest extends FluentRequest<FluentUnlockRequest,UnlockRequest,UnlockResponse> {}

    /**
     * Acquire a lock with the given name. Unless one of the {@code withLease()}
     * methods is used, the lock will be tied to this client's session lease.
     * <p>
     * The response contains a key whose existence is tied to the caller's ownership
     * of the lock. This key can be used in conjunction with transactions to safely
     * ensure updates to etcd only occur while holding lock ownership. The lock is held
     * until Unlock is called on the key or the lease associated with the owner expires.
     * 
     * @param name unique name identifying the lock
     */
    FluentLockRequest lock(ByteString name);

    /**
     * Release the lock with the given key.
     * 
     * @param key unique key returned in the {@link LockResponse}
     *   from the {@link #lock(ByteString)} method.
     */
    FluentUnlockRequest unlock(ByteString key);
}
