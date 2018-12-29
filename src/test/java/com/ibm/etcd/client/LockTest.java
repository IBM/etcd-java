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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.google.protobuf.ByteString;
import com.ibm.etcd.api.LeaseGrantResponse;
import com.ibm.etcd.api.LockResponse;
import com.ibm.etcd.client.kv.KvClient;
import com.ibm.etcd.client.kv.LockClient;
import com.ibm.etcd.client.lease.LeaseClient;
import com.ibm.etcd.client.lease.PersistentLease;

import io.grpc.Deadline;

public class LockTest {

    @Test
    public void testLocks() throws Exception {
        try(KvStoreClient client = EtcdClient.forEndpoint("localhost", 2379)
                .withPlainText().build()) {
            LockClient lc = client.getLockClient();
            KvClient kvc = client.getKvClient();
            LeaseClient lec = client.getLeaseClient();
            
            LockResponse lr = lc.lock(KeyUtils.bs("mylock"))
                    .deadline(Deadline.after(500, TimeUnit.MILLISECONDS)).sync();
            
            ByteString lockKey = lr.getKey();
            assertNotNull(lockKey);
            assertTrue(kvc.txnIf().exists(lockKey).sync().getSucceeded());
            
            assertNotNull(lc.unlock(lockKey).sync());
            assertFalse(kvc.txnIf().exists(lockKey).sync().getSucceeded());
            
            
            LeaseGrantResponse lgr = lec.grant(3L).sync();
            
            lr = lc.lock(KeyUtils.bs("mylock2")).withLease(lgr.getID()).sync();
            
            lockKey = lr.getKey();
            assertNotNull(lockKey);
            assertTrue(kvc.txnIf().exists(lockKey).sync().getSucceeded());
            
            assertNotNull(lc.unlock(lockKey).sync());
            assertFalse(kvc.txnIf().exists(lockKey).sync().getSucceeded());
            
            PersistentLease pl = lec.maintain().start();
            
            lr = lc.lock(KeyUtils.bs("mylock3")).withLease(pl).sync();
            
            lockKey = lr.getKey();
            assertNotNull(lockKey);
            assertTrue(kvc.txnIf().exists(lockKey).sync().getSucceeded());
            
            pl.close();
            
            Thread.sleep(500L); // don't currently have future for PL close unfortunately

            assertFalse(kvc.txnIf().exists(lockKey).sync().getSucceeded());
        }
    }
    
}
