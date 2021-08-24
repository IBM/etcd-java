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

import static com.ibm.etcd.client.KeyUtils.fromHexString;
import static com.ibm.etcd.client.KeyUtils.plusOne;
import static com.ibm.etcd.client.KeyUtils.toHexString;
import static com.ibm.etcd.client.KeyUtils.bs;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.protobuf.ByteString;
import com.ibm.etcd.api.KeyValue;
import com.ibm.etcd.api.PutResponse;
import com.ibm.etcd.api.RangeRequest.SortOrder;
import com.ibm.etcd.api.RangeRequest.SortTarget;
import com.ibm.etcd.client.EtcdClient;
import com.ibm.etcd.client.LocalNettyProxy;
import com.ibm.etcd.client.kv.KvClient;
import com.ibm.etcd.client.kv.KvClient.FluentTxnOps;
import com.ibm.etcd.client.utils.RangeCache.PutResult;

import io.grpc.Deadline;

public class RangeCacheTest {

    static LocalNettyProxy proxy;

    static EtcdClient client, directClient;

    @BeforeClass
    public static void setup() throws Exception {
        (proxy = new LocalNettyProxy(2394)).start();
        client = EtcdClient.forEndpoint("localhost", 2394)
                .withPlainText().build();
        directClient = EtcdClient.forEndpoint("localhost", 2379)
                .withPlainText().build();
    }

    @AfterClass
    public static void tearDown() throws Exception {
        if (client != null) {
            client.close();
        }
        if (directClient != null) {
            directClient.close();
        }
        if (proxy != null) {
            proxy.close();
        }
    }

    @Test
    public void testStrongIterator() throws Exception {

        KvClient kvc = directClient.getKvClient();

        kvc.delete(bs("sit-test/")).asPrefix().sync();
        kvc.delete(bs("sit-test")).sync();

        kvc.put(bs("sit-test/a"), bs("val")).sync();
        kvc.put(bs("sit-test/d"), bs("val")).sync();

        try (RangeCache rc = new RangeCache(directClient, bs("sit-test/"), false)) {

            //NOTE first RangeCache has NOT been started

            assertEquals(0, Iterators.size(rc.iterator()));

            assertEquals(2, Iterators.size(rc.strongIterator()));

            //assertEquals(2, Iterators.size(rc.iterator()));

            try (RangeCache rc2 = new RangeCache(directClient, bs("sit-test/"), false)) {

                rc2.delete(bs("sit-test/d"));

                assertEquals(0, Iterators.size(rc.iterator()));

                assertEquals(1, Iterators.size(rc.strongIterator()));

                //assertEquals(1, Iterators.size(rc.iterator()));
            }
        }
    }

   @Test
    public void testOrdering() throws Exception {
        KvClient kvc = client.getKvClient();
        ByteString px = bs("tmp1/"), val = bs("val");
        kvc.delete(px).asPrefix().sync();

        try (RangeCache rc = new RangeCache(client, px, true)) {
            rc.put(px.concat(fromHexString("ff")), val);
            rc.put(px.concat(fromHexString("02")), val);
            rc.put(px.concat(fromHexString("01")), val);
            rc.put(px.concat(fromHexString("0100")), val);
            rc.put(px.concat(fromHexString("00ff")), val);
            rc.put(px.concat(fromHexString("0101ab")), val);
            rc.put(px.concat(plusOne(fromHexString("01"))), val);

            List<KeyValue> directFromServer = kvc.get(px).asPrefix()
                    .sorted(SortTarget.KEY, SortOrder.ASCEND)
                    .sync().getKvsList();

            printKeys(directFromServer);
            printKeys(rc);
            System.out.println("--------");

            assertTrue(Iterables.elementsEqual(rc, directFromServer));
        }
    }
   
   private static void printKeys(Iterable<KeyValue> list) {
       System.out.println("--------");
       list.forEach(kv -> System.out.println(toHexString(kv.getKey())));
   }

    @Test
    public void testCompaction() throws Exception {
        KvClient kvc = client.getKvClient();

        Thread noise = new Thread(() -> {
            ByteString n = bs("/tmp/noise-");
            ByteString k = n.concat(bs("0"));
            for (int i = 0; i < 1000; i++) {
                kvc.delete(k).sync();
                k = n.concat(bs("" + i));
                PutResponse pr = kvc.put(k, n).sync();
                if (i == 500) {
                    kvc.compact(pr.getHeader().getRevision(), false);
                }
            }
        });

        try (RangeCache rc = new RangeCache(client, bs("tmp/"), false)) {
            rc.start().get(1L, TimeUnit.SECONDS);
                    noise.start();
                    noise.join();
        }
    }

    @Test
    public void testBasics() throws Exception {

        KvClient kvc = client.getKvClient();

        kvc.delete(bs("tmp/")).asPrefix().sync();

        try (RangeCache rc = new RangeCache(client, bs("tmp/"), false)) {

            PutResult pr = rc.put(bs("tmp/a"), bs("val1"), 0L);
            assertTrue(pr.succ());
            assertEquals(bs("val1"), pr.kv().getValue());

            rc.start().get(1L, TimeUnit.SECONDS);

            assertTrue(rc.delete(bs("tmp/a")));

            assertFalse(rc.delete(bs("tmp/c")));

            assertTrue(rc.put(bs("tmp/a"), bs("val1"), 0L).succ());

            assertEquals(bs("val1"), rc.get(bs("tmp/a")).getValue());

            assertEquals(bs("val1"), rc.getRemote(bs("tmp/a")).getValue());

            assertEquals(1, Iterators.size(rc.iterator()));

            Iterator<KeyValue> it = rc.iterator(), sit = rc.strongIterator();

            assertTrue(Iterators.elementsEqual(it, sit));

            KvClient directKv = directClient.getKvClient();

            directKv.put(bs("tmp/d"), bs("val2")).sync();

            Thread.sleep(80L);

            assertEquals(bs("val2"), rc.get(bs("tmp/d")).getValue());

            directKv.put(bs("tmp/d"), bs("valX")).sync();

            Thread.sleep(80L);

            assertEquals(bs("valX"), rc.get(bs("tmp/d")).getValue());

            directKv.delete(bs("tmp/d")).sync();

            Thread.sleep(80L);

            assertNull(rc.get(bs("tmp/d")));
        }
    }

    @Test
    public void testResiliency() throws Exception {

        directClient.getKvClient().delete(bs("tmp2/")).asPrefix().sync();

        try (EtcdClient rcClient = EtcdClient.forEndpoint("localhost", 2395)
                .withPlainText().build();
                RangeCache rc = new RangeCache(rcClient, bs("tmp2/"), false)) {

            try {
                // this won't finish starting because it's not connected
                rc.start().get(500, TimeUnit.MILLISECONDS);
            } catch (TimeoutException te) {
                // ok
            }

            Map<ByteString,ByteString> localMap = new HashMap<>();

            try (final LocalNettyProxy prox = new LocalNettyProxy(2395)) {

                Thread proxyThread = new Thread() {
                    { setDaemon(true); }
                    @Override public void run() {
                        try {
                            int N = 6;
                            for (int i = 1; i <= N; i++) {
                                prox.start();
                                Thread.sleep(1000L + (long) (Math.random() * 5000));
                                if (i < N) {
                                    System.out.println("killing proxy " + i);
                                    prox.kill(); // finish in running state
                                }
                                Thread.sleep((long) (Math.random() * 4000));
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                };
                proxyThread.start();

                KvClient directKv = directClient.getKvClient();

                int i = 0;
                // perform a bunch of direct updates to etcd while
                // the proxy is stopped/started
                while (proxyThread.isAlive()) {
                    // put a key
                    ByteString key = bs("tmp2/"+Math.random());
                    ByteString value = bs("value "+(i++));
                    directKv.put(key, value).sync();
                    localMap.put(key, value);
                    if (i > 5 && !localMap.isEmpty()) {
                        // delete a key
                        Thread.sleep((long) (Math.random() * 100));
                        ByteString randomKey = Iterables.get(localMap.keySet(),
                                (int) (Math.random() * localMap.size()));
                        directKv.delete(randomKey).sync();
                        localMap.remove(randomKey);
                        Thread.sleep((long) (Math.random() * 100));
                    }
                    if (i > 3) {
                        // perform batch update (3 puts, 1 delete)
                        FluentTxnOps<?> batch = directKv.batch();
                        if (!localMap.isEmpty()) {
                            ByteString randomKey = Iterables.get(localMap.keySet(),
                                    (int) (Math.random() * localMap.size()));
                            batch.delete(directKv.delete(randomKey).asRequest());
                            localMap.remove(randomKey);
                        }
                        for (int j = 0; j < 3; j++) {
                            key = bs("tmp2/"+Math.random());
                            value = bs("value "+(i++));
                            batch.put(directKv.put(key,value).asRequest());
                            localMap.put(key, value);
                        }
                        batch.sync(); // commit batch txn
                        Thread.sleep((long) (Math.random() * 100));
                    }
                }

                int ls = localMap.size(), rs = (int) directKv.get(bs("tmp2/"))
                        .asPrefix().countOnly().sync().getCount();
                System.out.println("local map size is " + localMap.size());

                System.out.println("remote size is " + rs);

                assertEquals(ls, rs);

                // wait until connected and to catch up
                rcClient.getKvClient().get(bs("tmp2/"))
                .deadline(Deadline.after(20, TimeUnit.SECONDS))
                .backoffRetry().sync();
                Thread.sleep(6_000L);

                System.out.println("rc size is " + rc.size());

                assertEquals(localMap.size(), rc.size());

                // check contents of cache == contents of local map
                assertEquals(localMap.entrySet(), Sets.newHashSet(Iterables.transform(rc, kv
                        -> Maps.immutableEntry(kv.getKey(), kv.getValue()))));

                testOfflineCompact(directKv, rc, prox);
                // Test compact recovery twice
                testOfflineCompact(directKv, rc, prox);
            }
        }
    }

    @Test
    public void testSizeStability() throws Exception {

        directClient.getKvClient().delete(bs("tmp3/")).asPrefix().sync();

        try (RangeCache rc = new RangeCache(directClient, bs("tmp3/"), false)) {
            rc.start().get();
            rc.put(bs("tmp3/key1"), bs("val1"));
            rc.put(bs("tmp3/key2"), bs("val2"));
            assertEquals(2, rc.size());
            assertNull(rc.getRemote(bs("tmp3/key3")));
            assertEquals(2, rc.size());
            rc.put(bs("tmp3/key3"), bs("val3"));
            assertEquals(3, rc.size());
        }
    }

    private static void testOfflineCompact(KvClient directKv, RangeCache rc, LocalNettyProxy prox) throws Exception {
        int i;
        directKv.delete(bs("tmp2/aftercompact")).sync();
        directKv.put(bs("tmp2/abc-0"), bs("def")).sync();
        directKv.delete(bs("tmp2/abc-1")).rangeEnd(bs("tmp2/abc-9")).sync();
        Thread.sleep(300L); // ensure cache catches up
        assertTrue(rc.keyExists(bs("tmp2/abc-0")));
        prox.kill();
        Thread.sleep(500L);
        directKv.delete(bs("tmp2/abc-0")).sync();
        for (i = 1; i < 8; i++) {
            directKv.put(bs("tmp2/abc-"+i), bs("def")).sync();
        }
        long rev = directKv.put(bs("tmp2/abc-8"), bs("def")).sync().getHeader().getRevision();
        directKv.put(bs("tmp2/abc-9"), bs("def")).sync();
        directKv.compact(rev, true).get();
        Thread.sleep(500L);
        // cache won't have seen the deletion yet
        assertTrue(rc.keyExists(bs("tmp2/abc-0")));
        System.out.println("starting after compaction");
        prox.start();
        // wait for cache to be refreshed
        for (i = 1; i <= 20; i++) {
            Thread.sleep(1000L);
            KeyValue kv1 = rc.get(bs("tmp2/abc-7"));
            KeyValue kv2 = rc.get(bs("tmp2/abc-8"));
            KeyValue kv3 = rc.get(bs("tmp2/abc-9"));
            boolean exists = rc.keyExists(bs("tmp2/abc-0"));
            if (kv1 == null || kv2 == null || kv3 == null || exists) {
                if (i == 20) {
                    fail("Cache did not catch up");
                }
                continue;
            }
            assertEquals(bs("def"), kv1.getValue());
            assertEquals(bs("def"), kv2.getValue());
            assertEquals(bs("def"), kv3.getValue());
            break;
        }

        directKv.put(bs("tmp2/aftercompact"), bs("val2")).sync();
        Thread.sleep(100L);
        assertEquals(bs("val2"), rc.get(bs("tmp2/aftercompact")).getValue());
    }

    @Test
    public void testListeners() {
        //TODO
    }

    static String str(KeyValue kv) {
        return kv.getValue().toStringUtf8();
    }

}
