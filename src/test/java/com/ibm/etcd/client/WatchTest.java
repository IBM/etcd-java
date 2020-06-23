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

import static com.ibm.etcd.client.KeyUtils.bs;
import static com.ibm.etcd.client.KvTest.t;
import static org.junit.Assert.*;

import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Phaser;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.protobuf.ByteString;
import com.ibm.etcd.api.Event.EventType;
import com.ibm.etcd.api.PutResponse;
import com.ibm.etcd.client.kv.KvClient;
import com.ibm.etcd.client.kv.KvClient.Watch;
import com.ibm.etcd.client.kv.KvClient.WatchIterator;
import com.ibm.etcd.client.kv.WatchUpdate;
import com.ibm.etcd.client.watch.WatchCreateException;

import io.grpc.stub.StreamObserver;

public class WatchTest {

    static LocalNettyProxy proxy;

    @BeforeClass
    public static void setup() {
        proxy = new LocalNettyProxy(2390);
    }
    @AfterClass
    public static void teardown() throws Exception {
        if (proxy != null) {
            proxy.close();
        }
    }

    @Test
    public void testWatch() throws Exception {

        proxy.start();

        try (KvStoreClient directClient = EtcdClient.forEndpoint("localhost", 2379)
                .withPlainText().build();
                KvStoreClient client = EtcdClient.forEndpoint("localhost", 2390)
                        .withPlainText().build()) {

            KvClient kvc = client.getKvClient();

            long start = System.currentTimeMillis();

            final BlockingQueue<Object> watchEvents = new LinkedBlockingQueue<>();

            final Object COMPLETED = new Object();

            final StreamObserver<WatchUpdate> observer = new StreamObserver<WatchUpdate>() {
                @Override
                public void onNext(WatchUpdate value) {
                    System.out.println(t(start) + "watch event: " + value);
                    watchEvents.add(value);
                }
                @Override
                public void onError(Throwable t) {
                    System.out.println(t(start) + "watch error: " + t);
                    watchEvents.add(t);
                }
                @Override
                public void onCompleted() {
                    System.out.println(t(start) + "watch completed");
                    watchEvents.add(COMPLETED);
                }
            };

            Watch watch = kvc.watch(bs("/watchtest")).asPrefix().start(observer);
            //assertFalse(watch.isDone());

            // test blocking watch at the same time
            WatchIterator watchIterator = kvc.watch(bs("/watchtest")).asPrefix().start();

            assertTrue(watch.get(1, TimeUnit.SECONDS));

            kvc.put(bs("/watchtest/a"), bs("a value")).sync();
            directClient.getKvClient().put(bs("/watchtest/b"), bs("b value")).sync();

            WatchUpdate wu = getNextSkipEmpty(watchEvents);
            assertNotNull(wu);
            assertEquals("event: " + wu, 1, wu.getEvents().size());
            assertEquals(EventType.PUT, wu.getEvents().get(0).getType());
            assertEquals(bs("a value"), wu.getEvents().get(0).getKv().getValue());
            assertEquals(bs("/watchtest/a"), wu.getEvents().get(0).getKv().getKey());

            WatchUpdate wu2 = getNextSkipEmpty(watchIterator);
            assertEquals(EventType.PUT, wu2.getEvents().get(0).getType());
            assertEquals(bs("a value"), wu2.getEvents().get(0).getKv().getValue());
            assertEquals(bs("/watchtest/a"), wu2.getEvents().get(0).getKv().getKey());

            watchEvents.poll(500, TimeUnit.MILLISECONDS);

            watch.close();

            assertEquals(COMPLETED, watchEvents.poll(500, TimeUnit.MILLISECONDS));

            kvc.put(bs("/watchtest/c"), bs("c value")).sync();

            assertNull(watchEvents.poll(500, TimeUnit.MILLISECONDS));

            assertTrue(watchIterator.hasNext());
            assertTrue(watchIterator.hasNext());
            assertEquals(bs("b value"), watchIterator.next()
                    .getEvents().get(0).getKv().getValue());

            assertEquals(EventType.PUT, watchIterator.next()
                    .getEvents().get(0).getType()); // PUT of "/watchtest/c"

            // fresh new watch
            watch = kvc.watch(ByteString.copyFromUtf8("/watchtest"))
                    .asPrefix().start(observer);

            assertTrue(watch.get(1, TimeUnit.SECONDS));

            kvc.batch().put(kvc.put(bs("/watchtest/d"), bs("d value")).asRequest())
            .delete(kvc.delete(bs("/watchtest/a")).asRequest()).sync();

            wu = getNextSkipEmpty(watchEvents);

            assertEquals(2, wu.getEvents().size());
            assertTrue(wu.getEvents().stream().anyMatch(e -> e.getType() == EventType.DELETE));
            assertTrue(wu.getEvents().stream().anyMatch(e -> e.getType() == EventType.PUT));

            // kill path to server
            proxy.kill();
            Thread.sleep(1000L);

            // while disconnected, put a new key
            directClient.getKvClient().put(bs("/watchtest/e"), bs("e value")).sync();
            Thread.sleep(1000L);

            // reinstate path to server
            proxy.start();

            // watch should be unaffected - next event seen should be the missed one
            wu = (WatchUpdate) watchEvents.poll(6000L, TimeUnit.MILLISECONDS);
            assertNotNull("Expected watch event not received after server reconnection", wu);
            assertEquals(bs("/watchtest/e"), wu.getEvents().get(0).getKv().getKey());

            watch.close();

            assertEquals(2, watchIterator.next().getEvents().size()); // (earlier batch update)
            assertEquals(bs("/watchtest/e"), watchIterator.next()
                    .getEvents().get(0).getKv().getKey());

            watchIterator.close();
            assertNull(watchIterator.next().getHeader());

            assertFalse(watchIterator.hasNext());

            try {
                watchIterator.next();
                fail("should throw NSEE here");
            } catch (NoSuchElementException nsee) {}

        } finally {
            proxy.kill();
        }
    }

    static WatchUpdate getNextSkipEmpty(BlockingQueue<Object> watchEvents) throws InterruptedException {
        WatchUpdate wu = (WatchUpdate) watchEvents.poll(500, TimeUnit.MILLISECONDS);
        if (wu.getEvents().isEmpty()) {
            wu = (WatchUpdate) watchEvents.poll(500, TimeUnit.MILLISECONDS);
        }
        return wu;
    }

    static WatchUpdate getNextSkipEmpty(WatchIterator wi) {
        assertTrue(wi.hasNext());
        WatchUpdate wu = wi.next();
        if (wu.getEvents().isEmpty()) {
            assertTrue(wi.hasNext());
            wu = wi.next();
        }
        return wu;
    }

    @Test
    public void testResiliency() throws Exception {

        try (EtcdClient directClient = EtcdClient.forEndpoint("localhost", 2379)
                .withPlainText().build();
                EtcdClient client = EtcdClient.forEndpoint("localhost", 2396)
                        .withPlainText().build()) {

            directClient.getKvClient().delete(bs("watch-tr-test/")).asPrefix().sync();

            try (final LocalNettyProxy prox = new LocalNettyProxy(2396)) {

                Thread proxyThread = new Thread() {
                    { setDaemon(true); }
                    @Override public void run() {
                        try {
                            int N = 4;
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

                Phaser p = new Phaser(1);
                Map<ByteString,Object> watchedKeys = new ConcurrentHashMap<>();

                int i = 0;
                // perform a bunch of watch creations/cancellations while
                // the proxy is stopped/started
                while (proxyThread.isAlive()) {
                    // put a key
                    ByteString key = bs("watch-tr-test/" + Math.random());
                    ByteString value = bs("value " + (i++));
                    PutResponse pr = directKv.put(key, value).sync();
                    watchedKeys.put(key, "pending");
                    p.register();

                    final int ii = i;
                    AtomicReference<Watch> w = new AtomicReference<>();
                    w.set(client.getKvClient().watch(key)
                            .startRevision(pr.getHeader().getRevision())
                            .start((ListenerObserver<WatchUpdate>) (complete, wu, err) -> {
                                if (complete) {
                                    if (err != null) {
                                        watchedKeys.replace(key, err);
                                        err.printStackTrace();
                                    } else {
                                        watchedKeys.remove(key);
                                    }
                                    if (w.get() == null) {
                                        p.arrive();
                                    }
                                } else if (!wu.getEvents().isEmpty()) {
                                    if (value.equals(wu.getEvents().get(0).getKv().getValue())) {
                                        if (ii % 2 == 0) {
                                            // cancel every other watch
                                            w.get().close();
                                            w.set(null);
                                        } else {
                                            watchedKeys.remove(key);
                                            p.arrive();
                                        }
                                    } else {
                                        watchedKeys.replace(key, "unexpected watch event value");
                                        p.arrive();
                                    }
                                }
                            }));
                    Thread.sleep((long) (Math.random() * 500));
                }

                p.arrive();
                p.awaitAdvanceInterruptibly(0, 10, TimeUnit.SECONDS);

                System.out.println("created " + i + " watches; left incomplete: " + watchedKeys.size());

                watchedKeys.entrySet().forEach(e ->
                System.out.println("** " + e.getKey().toStringUtf8() + "=" + e.getValue()));

                assertTrue(watchedKeys.isEmpty());
            }
        }
    }

    @Test
    public void testCreateFail() throws Exception {
        KvStoreClient client = EtcdClient.forEndpoint("localhost", 2379)
                .withPlainText().build();
        try {
            KvClient kvc = client.getKvClient();
            // range end before start => should fail
            Watch watch2 = kvc.watch(ByteString.copyFromUtf8("/watchtest2"))
                    .rangeEnd(ByteString.copyFromUtf8("/watchtest1"))
                    .startRevision(-5000L).start(
                            (ListenerObserver<WatchUpdate>) (c,v,t) -> {});
            try {
                watch2.get(1000, TimeUnit.SECONDS);
                fail("watch future should fail");
            } catch (ExecutionException e) {
                System.out.println("watch creation failed with exception: " + e);
                assertTrue(e.getCause() instanceof WatchCreateException);
            }
        } finally {
            client.close();
        }
    }

}
