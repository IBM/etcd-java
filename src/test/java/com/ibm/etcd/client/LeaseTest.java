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

import static org.junit.Assert.*;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

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
        if(proxy != null) proxy.close();
    }
    
    @Test
    public void testPersistentLease() throws Exception {
        
        proxy.start();

        try(KvStoreClient directClient = EtcdClient.forEndpoint("localhost", 2379)
                .withPlainText().build();
            KvStoreClient client = EtcdClient.forEndpoint("localhost", 2393)
                .withPlainText().build()) {

            LeaseClient lc = client.getLeaseClient();

            long start = System.currentTimeMillis();

            final BlockingQueue<Object> observerEvents = new LinkedBlockingQueue<>();

            final Object COMPLETED = new Object();
            
            int minTtl = 5, kaFreq = 4;

            PersistentLease pl = lc.maintain().minTtl(minTtl).keepAliveFreq(kaFreq)
                    .start(new StreamObserver<LeaseState>() {
                        @Override
                        public void onNext(LeaseState value) {
                            System.out.println(t(start)+"PL state change: "+value);
                            observerEvents.add(value);
                        }
                        @Override
                        public void onError(Throwable t) {
                            System.out.println(t(start)+"PL error: "+t);
                            observerEvents.add(t);
                        }
                        @Override
                        public void onCompleted() {
                            System.out.println(t(start)+"PL completed");
                            observerEvents.add(COMPLETED);
                        }
                    });

            long newId = pl.get(3L, TimeUnit.SECONDS);

            assertEquals(LeaseState.ACTIVE, pl.getState());
            System.out.println(t(start)+"new lease id: "+newId);

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
            assertEquals(LeaseState.ACTIVE_NO_CONN,
                    observerEvents.poll(2, TimeUnit.SECONDS));
            
            proxy.start();
            
            // should go back to active
            assertEquals(LeaseState.ACTIVE,
                    observerEvents.poll(4, TimeUnit.SECONDS));
            
            Thread.sleep(500L);
            System.out.println("ttl now "+pl.getCurrentTtlSecs()+"s");
            assertTrue(pl.getCurrentTtlSecs() > minTtl);
            
            proxy.kill();
            long afterKill = System.nanoTime();
            assertEquals(LeaseState.ACTIVE_NO_CONN,
                    observerEvents.poll(2, TimeUnit.SECONDS));
            
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
                    observerEvents.poll(minTtl+kaFreq, TimeUnit.SECONDS));
            
            long expiredMs = (System.nanoTime()-afterKill)/1000_000L;
            System.out.println("expired after "+expiredMs+"ms");
            
            // make sure it lasted at least minTtl
            assertTrue("expired too quickly", expiredMs >= minTtl*1000L);
            
            assertFalse(pl2.isDone()); // second lease still waiting
            
            proxy.start();
            long before = System.currentTimeMillis();
            
            // second lease should now become active
            long newLeaseId = pl2.get(20, TimeUnit.SECONDS);
            assertTrue(newLeaseId > 0L);
            assertNotEquals(pl.getLeaseId(), newLeaseId);
            assertEquals(LeaseState.ACTIVE, pl2.getState());
            
            // should go back to active after expired
            assertEquals(LeaseState.ACTIVE,
                    observerEvents.poll(10, TimeUnit.SECONDS));
            System.out.println("took "+(System.nanoTime()-before)/1000_000L
                    +"ms to become active again");

            pl.close();
            pl2.close();

            assertEquals(LeaseState.CLOSED,
                    observerEvents.poll(1, TimeUnit.SECONDS));
            assertEquals(COMPLETED, observerEvents.poll(1, TimeUnit.SECONDS));
            assertNull(observerEvents.poll(500, TimeUnit.MILLISECONDS));
            assertEquals(LeaseState.CLOSED, pl.getState());
            assertEquals(0, pl.getCurrentTtlSecs());

            assertEquals(-1L, lc.ttl(newId).get().getTTL());

        } finally {
            proxy.kill();
        }
    }
    
    static String t(long start) {
        return String.format("%.3f ", (System.currentTimeMillis()-start)/1000.0);
    }

}
