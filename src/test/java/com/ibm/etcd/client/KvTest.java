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

import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.ByteString;
import com.ibm.etcd.client.EtcdClient;
import com.ibm.etcd.client.KvStoreClient;
import com.ibm.etcd.client.kv.KvClient;

import io.grpc.Deadline;
import io.grpc.Status;
import io.grpc.Status.Code;

import com.ibm.etcd.api.KeyValue;
import com.ibm.etcd.api.PutRequest;
import com.ibm.etcd.api.RangeResponse;
import com.ibm.etcd.api.TxnResponse;

public class KvTest {

    static LocalNettyProxy proxy;

    @BeforeClass
    public static void setup() {
        proxy = new LocalNettyProxy(2391);
    }
    @AfterClass
    public static void teardown() throws Exception {
        if (proxy != null) {
            proxy.close();
        }
    }

    @Test
    public void testKvOps() throws Exception {

        proxy.start();

        try (KvStoreClient directClient = EtcdClient.forEndpoint("localhost", 2379)
                .withPlainText().build();
                KvStoreClient client = EtcdClient.forEndpoint("localhost", 2391)
                        .withPlainText().build()) {

            KvClient kvc = client.getKvClient();

            ByteString a = bs("a"), b = bs("b"), v1 = bs("v1"), v2 = bs("v2");

            // clean up first
            kvc.batch().delete(kvc.delete(a).asRequest())
            .delete(kvc.delete(b).asRequest()).sync();

            // basic put
            assertEquals(KeyValue.getDefaultInstance(),
                    kvc.put(a, v2).prevKv().sync().getPrevKv());
            assertTrue(kvc.put(a, v2).sync().getHeader().getRevision() > 0);
            assertEquals(v2, kvc.put(a, v1).prevKv().sync().getPrevKv().getValue());

            // basic get
            RangeResponse rr = kvc.get(bs("a")).sync();
            assertEquals(1L, rr.getCount());
            assertEquals(v1, rr.getKvs(0).getValue());

            // basic delete
            assertEquals(0L, kvc.delete(bs("notthere")).sync().getDeleted());
            assertEquals(v1, kvc.delete(a).prevKv().sync().getPrevKvs(0).getValue());
            assertEquals(0, kvc.get(bs("a")).sync().getCount());

            PutRequest pr1 = kvc.put(a, v1).asRequest(), pr2 = kvc.put(b, v2).asRequest();

            // batch put
            assertEquals(2, kvc.batch().put(pr1).put(pr2).sync().getResponsesCount());

            assertEquals(v1, kvc.get(a).sync().getKvs(0).getValue());
            assertEquals(v2, kvc.get(b).sync().getKvs(0).getValue());

            // basic transaction
            ListenableFuture<TxnResponse> tresp = kvc.txnIf().cmpEqual(a).value(v1)
                    .and().cmpNotEqual(b).version(10)
                    .then().put(kvc.put(bs("new"), bs("newval")).asRequest()).async();

            assertNotNull(tresp.get().getResponses(0).getResponsePut()
                    .getHeader());

            // test disconnected behaviour
            proxy.kill();
            Thread.sleep(200L);

            ListenableFuture<RangeResponse> rrFut1 = kvc.get(bs("new")).async(); // should fail
            ListenableFuture<RangeResponse> rrFut2 = kvc.get(bs("new"))
                    .deadline(Deadline.after(20, TimeUnit.SECONDS))
                    .backoffRetry().async(); // should work

            try {
                rrFut1.get(1000, TimeUnit.SECONDS);
                fail("expected get to fail while disconnected");
            } catch (Exception e) {
                System.out.println("failed with: " + e); //TODO
            }

            // this one should still be retrying
            assertFalse(rrFut2.isDone());

            // reconnect
            proxy.start();

            // should succeed once network path is there again
            long before = System.nanoTime();
            RangeResponse rr2 = rrFut2.get(2000, TimeUnit.SECONDS);
            long took = (System.nanoTime() - before) / 1000_000L;
            assertEquals(bs("newval"), rr2.getKvs(0).getValue());
            System.out.println("took "+took+"ms after network was reestablished");

        } finally {
            proxy.close();
        }
    }

    @Test(timeout=15000)
    public void testSyncDeadlock() throws Exception {

        // see https://github.com/IBM/etcd-java/issues/16

        final long DEFAULT_TIMEOUT_MS = 1L;
        final long DEADLINE_TIMEOUT_MS = 1L;

        try (KvStoreClient client = EtcdClient.forEndpoint("localhost", 2379)
                .withPlainText().build()) {

            client.getKvClient().delete(bs("deadlock-test/")).asPrefix().sync();
            try {
                IntStream.range(0, 6000).forEach(i -> {
                    try {
                        client.getKvClient()
                        .put(bs("deadlock-test/" + UUID.randomUUID().toString()), bs("some-value"))
                        .timeout(DEFAULT_TIMEOUT_MS)
                        .backoffRetry()
                        .deadline(Deadline.after(DEADLINE_TIMEOUT_MS, TimeUnit.MILLISECONDS))
                        .sync();
                    } catch (Exception e) {
                        Status status = Status.fromThrowable(e);
                        if (Code.DEADLINE_EXCEEDED != status.getCode()) {
                            System.out.println("attempt " + i + " returned " + status);
                            // etcdserver seems to return UNAVAILABLE instead of
                            // DEADLINE_EXEEDED for deadline breach
                            assertEquals("attept " + i, Code.UNAVAILABLE, status.getCode());
                            assertTrue(status.getDescription().endsWith("request timed out"));
                        }
                    }
                });
            } finally {
                // clean up
                Thread.interrupted();
                client.getKvClient().delete(bs("deadlock-test/")).asPrefix().sync();
            }
        }
    }

    public static ByteString bs(String str) {
        return ByteString.copyFromUtf8(str);
    }

    static String t(long start) {
        return String.format("%.3f ", (System.currentTimeMillis() - start) / 1000.0);
    }

}
