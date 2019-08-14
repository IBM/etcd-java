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

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.*;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Sets;
import com.ibm.etcd.api.LeaseGrantResponse;
import com.ibm.etcd.api.LeaseKeepAliveResponse;
import com.ibm.etcd.api.LeaseStatus;
import com.ibm.etcd.client.lease.LeaseClient;
import com.ibm.etcd.client.lease.PersistentLease;
import com.ibm.etcd.client.lease.PersistentLease.LeaseState;

import io.grpc.stub.StreamObserver;

public class LeaseTest {

    static LocalNettyProxy proxy;

    @BeforeClass
    public static void setup() {
        proxy = new LocalNettyProxy(2393);
    }
    @AfterClass
    public static void teardown() throws Exception {
        if (proxy != null) {
            proxy.close();
        }
    }

    @Test
    public void testOneTimeOperations() throws Exception {
        try (KvStoreClient directClient = EtcdClient.forEndpoint("localhost", 2379)
                .withPlainText().build()) {
            LeaseClient leaseClient = directClient.getLeaseClient();

            // keepalive for non-existent
            LeaseKeepAliveResponse lkar = leaseClient.keepAliveOnce(123L).get(2, SECONDS);
            assertEquals(123L, lkar.getID());
            assertEquals(0L, lkar.getTTL());

            // grant
            LeaseGrantResponse lgr = leaseClient.grant(5L).sync();
            assertEquals(5L, lgr.getTTL());
            long id = lgr.getID();
            assertNotEquals(0L, id);

            lkar = leaseClient.keepAliveOnce(id).get(1, SECONDS);
            assertEquals(5L, lkar.getTTL());
            assertEquals(id, lkar.getID());

            lgr = leaseClient.grant(10L).leaseId(456L).sync();
            assertEquals(10L, lgr.getTTL());
            assertNotEquals(456L, id);
            long ttl = leaseClient.ttl(456L).get(1, SECONDS).getTTL();
            assertTrue("was " + ttl, ttl == 9L || ttl == 10L);

            // revoke
            assertNotNull(leaseClient.revoke(id).get(1, SECONDS));

            assertEquals(-1L, leaseClient.ttl(id).get().getTTL());

            PersistentLease pl = leaseClient.maintain().start();
            pl.get(1, SECONDS);

            // test keepalive of existing lease while there is a PL open
            // (should share existing stream)
            assertEquals(10L, leaseClient.keepAliveOnce(456L).get(1, SECONDS).getTTL());

            pl.close();
            Thread.sleep(500L);
            assertEquals(LeaseState.CLOSED, pl.getState());

            // test keepalive of existing lease still works
            // (should go back to opening a new stream)
            assertEquals(10L, leaseClient.keepAliveOnce(456L).get(1, SECONDS).getTTL());

            assertNotNull(leaseClient.revoke(456L).get(1, SECONDS));
        }
    }

    @Test
    public void testPersistentLease() throws Exception {

        proxy.start();

        try (KvStoreClient directClient = EtcdClient.forEndpoint("localhost", 2379)
                .withPlainText().build();
                KvStoreClient client = EtcdClient.forEndpoint("localhost", 2393)
                        .withPlainText().build()) {

            LeaseClient lc = client.getLeaseClient();

            long start = System.currentTimeMillis();

            final BlockingQueue<Object> observerEvents = new LinkedBlockingQueue<>();

            final Object COMPLETED = new Object();

            int minTtl = 6, kaFreq = 4;

            PersistentLease pl = lc.maintain().minTtl(minTtl).keepAliveFreq(kaFreq)
                    .start(new StreamObserver<LeaseState>() {
                        @Override
                        public void onNext(LeaseState value) {
                            System.out.println(t(start) + "PL state change: " + value);
                            observerEvents.add(value);
                        }
                        @Override
                        public void onError(Throwable t) {
                            System.out.println(t(start) + "PL error: " + t);
                            observerEvents.add(t);
                        }
                        @Override
                        public void onCompleted() {
                            System.out.println(t(start) + "PL completed");
                            observerEvents.add(COMPLETED);
                        }
                    });

            long newId = pl.get(3L, SECONDS);

            assertEquals(LeaseState.ACTIVE, pl.getState());
            System.out.println(t(start) + "new lease id: " + newId);

            assertEquals(minTtl+kaFreq, pl.getPreferredTtlSecs());
            assertTrue(newId > 0L);
            assertEquals(newId, pl.getLeaseId());
            assertEquals(LeaseState.ACTIVE, observerEvents.poll(200, TimeUnit.MILLISECONDS));
            assertNull(observerEvents.poll());

            Thread.sleep(2000L);
            assertTrue(Math.abs(lc.ttl(newId).get().getTTL() - pl.getCurrentTtlSecs()) <= 1);

            Thread.sleep(4000L);

            assertNull(observerEvents.poll());
            assertTrue(Math.abs(lc.ttl(newId).get().getTTL() - pl.getCurrentTtlSecs()) <= 1);
            assertEquals(LeaseState.ACTIVE, pl.getState());

            // cut the cord
            proxy.kill();

            // state should reflect disconnection
            assertEquals(LeaseState.ACTIVE_NO_CONN, observerEvents.poll(2, SECONDS));

            proxy.start();

            // should go back to active
            assertEquals(LeaseState.ACTIVE, observerEvents.poll(6, SECONDS));

            Thread.sleep(500L);
            System.out.println("ttl now " + pl.getCurrentTtlSecs() + "s");
            assertTrue(pl.getCurrentTtlSecs() > minTtl);

            proxy.kill();
            long afterKill = System.nanoTime();
            assertEquals(LeaseState.ACTIVE_NO_CONN, observerEvents.poll(2, SECONDS));

            // test creating 2nd lease while disconnected
            PersistentLease pl2 = lc.maintain().minTtl(minTtl).keepAliveFreq(kaFreq)
                    .start();
            // should stay in pending state
            assertEquals(LeaseState.PENDING, pl2.getState());
            Thread.sleep(500L);
            assertEquals(LeaseState.PENDING, pl2.getState());
            assertEquals(0L, pl2.getLeaseId());

            // wait for expiry
            assertEquals(LeaseState.EXPIRED,
                    observerEvents.poll(minTtl + kaFreq, SECONDS));

            long expiredMs = (System.nanoTime( )- afterKill) / 1000_000L;
            System.out.println("expired after " + expiredMs + "ms");

            // make sure it lasted at least minTtl
            assertTrue("expired too quickly", expiredMs >= minTtl*1000L);

            assertFalse(pl2.isDone()); // second lease still waiting

            proxy.start();
            long before = System.currentTimeMillis();

            // second lease should now become active
            long newLeaseId = pl2.get(20, SECONDS);
            assertTrue(newLeaseId > 0L);
            assertNotEquals(pl.getLeaseId(), newLeaseId);
            assertEquals(LeaseState.ACTIVE, pl2.getState());

            // should go back to active after expired
            assertEquals(LeaseState.ACTIVE, observerEvents.poll(10, SECONDS));
            System.out.println("took " + (System.nanoTime() - before) / 1000_000L
                    + "ms to become active again");

            pl.close();
            pl2.close();

            assertEquals(LeaseState.CLOSED, observerEvents.poll(1, SECONDS));
            assertEquals(COMPLETED, observerEvents.poll(1, SECONDS));
            assertNull(observerEvents.poll(500, TimeUnit.MILLISECONDS));
            assertEquals(LeaseState.CLOSED, pl.getState());
            assertEquals(0, pl.getCurrentTtlSecs());

            assertEquals(-1L, lc.ttl(newId).get().getTTL());

        } finally {
            proxy.kill();
        }
    }

    @Test
    public void testPersistentLeaseClientShutdown() throws Exception {
        try (EtcdClient client1 = EtcdClient.forEndpoint("localhost", 2379)
                .withPlainText().build();
                EtcdClient client2 = EtcdClient.forEndpoint("localhost", 2379)
                        .withPlainText().build()) {
            Set<PersistentLease> pls = new HashSet<>();
            for (int i = 0; i < 500; i++) {
                pls.add(client1.getLeaseClient()
                        .maintain().keepAliveFreq(4).minTtl(40).start());
            }
            Set<Long> lids = new HashSet<>();
            for (PersistentLease pl : pls) {
                Long lid = pl.get();
                assertTrue(lid > 0);
                lids.add(lid);
            }

            Set<Long> lidsFound = client2.getLeaseClient().list().get()
                    .getLeasesList().stream().map(LeaseStatus::getID).collect(Collectors.toSet());
            assertTrue(lidsFound.containsAll(lids));

            client1.close();
            client1.getInternalExecutor().awaitTermination(3, SECONDS);

            lidsFound = client2.getLeaseClient().list().get()
                    .getLeasesList().stream().map(LeaseStatus::getID).collect(Collectors.toSet());
            assertTrue(Sets.intersection(lids, lidsFound).isEmpty());
        }
    }

    static String t(long start) {
        return String.format("%.3f ", (System.currentTimeMillis() - start) / 1000.0);
    }

}
