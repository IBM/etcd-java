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
import static org.junit.Assert.assertEquals;

import java.util.Collections;

import org.junit.Test;

import com.ibm.etcd.client.kv.KvClient;

public class ClientBuilderTest {

    @Test
    public void testForEndpoint() throws Exception {
        try (KvStoreClient client = EtcdClient.forEndpoint("localhost", 2379)
                .withPlainText().build()) {
            basicTest(client);
        }
    }

    @Test
    public void testForEndpointsSingle() throws Exception {
        String[] endpoints = { "localhost:2379", "http://localhost:2379",
                "https://localhost:2379", "dns:///localhost:2379" };

        for (String endpoint : endpoints) {
            try (KvStoreClient client = EtcdClient.forEndpoints(Collections.singletonList(endpoint))
                    .withPlainText().build()) {
                basicTest(client);
            }
        }
    }

    @Test
    public void testForEndpointsMulti() throws Exception {
        try (KvStoreClient client = EtcdClient.forEndpoints(
                "localhost:2379,http://localhost:2379,https://localhost:2379,dns:///localhost:2379")
                .withPlainText().build()) {
            basicTest(client);
        }
    }

    static void basicTest(KvStoreClient client) {
        KvClient kvc = client.getKvClient();
        kvc.put(bs("cbt"), bs("test")).sync();
        assertEquals("test", kvc.delete(bs("cbt")).prevKv().sync()
                .getPrevKvs(0).getValue().toStringUtf8());
    }
}
