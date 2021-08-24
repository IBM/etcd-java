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
package com.ibm.etcd.client;

import java.io.Closeable;

import com.ibm.etcd.client.kv.KvClient;
import com.ibm.etcd.client.lease.LeaseClient;
import com.ibm.etcd.client.lease.PersistentLease;
import com.ibm.etcd.client.lock.LockClient;

public interface KvStoreClient extends Closeable {

    KvClient getKvClient();

    LeaseClient getLeaseClient();

    LockClient getLockClient();

    /**
     * Returns a singular {@link PersistentLease} bound
     * to the life of this client instance. May be lazily
     * established, but is revoked only when this client
     * instance is closed (closing the returned lease has
     * no effect).
     * 
     * @return the session lease
     */
    PersistentLease getSessionLease();

}
