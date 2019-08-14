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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.protobuf.ByteString;
import com.ibm.etcd.api.LeaseGrantResponse;
import com.ibm.etcd.api.LockResponse;
import com.ibm.etcd.client.kv.KvClient;
import com.ibm.etcd.client.lease.LeaseClient;
import com.ibm.etcd.client.lease.PersistentLease;
import com.ibm.etcd.client.lock.LockClient;

import io.grpc.Deadline;
import io.grpc.Status.Code;
import io.grpc.StatusRuntimeException;

public class LockTest {

    static KvStoreClient client;

    static LockClient lockClient;
    static KvClient kvClient;
    static LeaseClient leaseClient;

    @BeforeClass
    public static void setup() {
        client = EtcdClient.forEndpoint("localhost", 2379)
                .withPlainText().build();
        lockClient = client.getLockClient();
        kvClient = client.getKvClient();
        leaseClient = client.getLeaseClient();
    }
    @AfterClass
    public static void teardown() throws Exception {
        if (client != null) {
            client.close();
        }
    }

    @Test
    public void testWithSessionLease() throws Exception {
        LockResponse lr = lockClient.lock(KeyUtils.bs("mylock"))
                .deadline(Deadline.after(500, TimeUnit.MILLISECONDS)).sync();

        ByteString lockKey = lr.getKey();
        assertNotNull(lockKey);
        assertTrue(kvClient.txnIf().exists(lockKey).sync().getSucceeded());

        assertNotNull(lockClient.unlock(lockKey).sync());
        assertFalse(kvClient.txnIf().exists(lockKey).sync().getSucceeded());
    }

    @Test
    public void testWithLeaseId() throws Exception {
        LeaseGrantResponse lgr = leaseClient.grant(3L).sync();

        LockResponse lr = lockClient.lock(KeyUtils.bs("mylock2")).withLease(lgr.getID()).sync();

        ByteString lockKey = lr.getKey();
        assertNotNull(lockKey);
        assertTrue(kvClient.txnIf().exists(lockKey).sync().getSucceeded());

        assertNotNull(lockClient.unlock(lockKey).sync());
        assertFalse(kvClient.txnIf().exists(lockKey).sync().getSucceeded());
    }

    @Test
    public void testWithPersistentLease() throws Exception {
        LeaseClient lec = client.getLeaseClient();

        PersistentLease pl = lec.maintain().start();

        LockResponse lr = lockClient.lock(KeyUtils.bs("mylock3")).withLease(pl).sync();

        ByteString lockKey = lr.getKey();
        assertNotNull(lockKey);
        assertTrue(kvClient.txnIf().exists(lockKey).sync().getSucceeded());

        pl.close();

        Thread.sleep(500L); // don't currently have future for PL close unfortunately

        assertFalse(kvClient.txnIf().exists(lockKey).sync().getSucceeded());
    }

    @Test
    public void testWithContention() throws Exception {
        ByteString lockName = KeyUtils.bs(UUID.randomUUID().toString());

        LeaseGrantResponse lgr1 = leaseClient.grant(120L).sync(), lgr2 = leaseClient.grant(120L).sync();

        // lock with first lease
        LockResponse lr = lockClient.lock(lockName).withLease(lgr1.getID()).sync();

        // attempt lock with second one, should timeout
        long nanosBefore = System.nanoTime();
        try {
            LockResponse lr2 = lockClient.lock(lockName).withLease(lgr2.getID())
                    .deadline(Deadline.after(100, TimeUnit.MILLISECONDS)).sync();
            fail("second lock attempt should timeout");
        } catch (StatusRuntimeException sre) {
            assertEquals(Code.DEADLINE_EXCEEDED, sre.getStatus().getCode());
        }
        long tookMs = (System.nanoTime() - nanosBefore) / 1000_000L;
        assertTrue(tookMs > 50 && tookMs < 150);
    }

}
