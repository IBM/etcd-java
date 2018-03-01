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
package com.ibm.etcd.client.utils;

import static org.junit.Assert.*;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.ibm.etcd.client.KvTest.bs;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.ByteString;
import com.ibm.etcd.api.Event;
import com.ibm.etcd.api.KeyValue;
import com.ibm.etcd.client.EtcdClient;
import com.ibm.etcd.client.LocalNettyProxy;
import com.ibm.etcd.client.kv.KvClient;
import com.ibm.etcd.client.kv.KvClient.WatchIterator;
import com.ibm.etcd.client.lease.PersistentLease;

public class PersistentLeaseKeyTest {
    
    static LocalNettyProxy proxy;
    
    @BeforeClass
    public static void setup() {
        proxy = new LocalNettyProxy(2392);
    }
    @AfterClass
    public static void teardown() throws Exception {
        if(proxy != null) proxy.close();
    }

    @Test
    public void testLeaseKey() throws Exception {

        try(EtcdClient client = EtcdClient.forEndpoint("localhost",2392).withPlainText().build();
                EtcdClient directClient = EtcdClient.forEndpoint("localhost",2379)
                        .withPlainText().build()) {

            KvClient directKvClient = directClient.getKvClient();
            
            ByteString mykey = bs("mykeyy");
            PersistentLeaseKey plk = new PersistentLeaseKey(client, mykey, bs("defaultdata"), null);

            ListenableFuture<ByteString> startFuture = plk.startWithFuture();

            Thread.sleep(300L);
            // network conn to server not established yet
            assertFalse(startFuture.isDone());

            // key won't exist yet
            assertEquals(0, directKvClient.get(mykey).countOnly().sync().getCount());

            // reestablish network
            proxy.start();

            assertEquals(mykey, startFuture.get(3000, MILLISECONDS));

            // check key is present via other client
            assertEquals(bs("defaultdata"), directKvClient.get(mykey).sync().getKvs(0).getValue());

            plk.closeWithFuture().get(500, MILLISECONDS);

            // key should now be gone
            assertEquals(0, directKvClient.get(bs("mykeyy")).countOnly().sync().getCount());

            // -----------------
            
            PersistentLease pl = client.getLeaseClient().maintain()
                    .minTtl(2).start();
            System.out.println("PL state: "+pl.getState());
            plk = new PersistentLeaseKey(client, pl, mykey, bs("somedata"), null);
            plk.start();
            assertFalse(pl.isDone());
            Long leaseId = pl.get(1, TimeUnit.SECONDS);
            assertNotNull(leaseId);
            System.out.println("PL state: "+pl.getState());
            // will take a small amount of time after lease is created for PLK to be
            // created
            assertFalse(plk.isDone());
            plk.get(1, TimeUnit.SECONDS);
            KeyValue kv = directKvClient.get(mykey).sync().getKvs(0);
            assertEquals(bs("somedata"), kv.getValue());
            assertEquals((long)leaseId, kv.getLease());
            plk.setDefaultValue(bs("updateddata"));
            Thread.sleep(200L);
            // data doesn't change until key has to be recreated
            assertEquals(bs("somedata"), directKvClient
                    .get(mykey).sync().getKvs(0).getValue());
            
            proxy.kill();
            long ttl = pl.getCurrentTtlSecs();
            System.out.println("TTL after kill is "+ttl);
            Thread.sleep(1000L);
            // key should still be there (lease not yet expired)
            kv = directKvClient.get(mykey).sync().getKvs(0);
            assertEquals(bs("somedata"), kv.getValue());
            assertEquals((long)leaseId, kv.getLease());
            
            Thread.sleep((ttl+1) * 1000L);
            // lease should have now expired and key should be gone
            assertEquals(0, directKvClient.get(bs("mykeyy")).sync().getCount());
            
            proxy.start();
            long before = System.nanoTime();
            try(WatchIterator it = directKvClient.watch(bs("mykeyy")).start()) {
                for(int i=0;i<5;i++) {
                    List<Event> events = it.next().getEvents();
                    if(!events.isEmpty()) {
                        // key should be updated with new value once
                        // connection is reestablished
                        assertEquals(bs("updateddata"),
                                events.get(0).getKv().getValue());
                        System.out.println("took "+(System.nanoTime()-before)/1000_000L
                                +"ms for key to re-appear");
                        break;
                    }
                }
            }
            
        }
    }
    
}
