/*
 * Copyright 2017, 2020 IBM Corp. All Rights Reserved.
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
import static org.junit.Assert.assertTrue;

import java.nio.charset.StandardCharsets;

import org.junit.Test;

import com.google.common.io.ByteSource;
import com.google.protobuf.ByteString;
import com.ibm.etcd.api.KeyValue;
import com.ibm.etcd.api.RangeResponse;
import com.ibm.etcd.client.config.EtcdClusterConfig;
import com.ibm.etcd.client.kv.KvClient;

import io.grpc.StatusRuntimeException;

/**
 * Current certain etcd servers are running with appropriate security configs.
 * This is arranged by running within {@link EtcdTestSuite}.
 */
public class JsonConfigTest {

    private static ByteSource makeJson(String endpoint, String certFile,
                                       String clientKeyFile, String clientCertFile) throws Exception {
        String json = "{ \"endpoints\": \"" + endpoint + "\""
                + (certFile != null ? ", \"certificate_file\": \"" + certFile + "\"" : "")
                + (clientKeyFile != null ? ", \"client_key_file\": \"" + clientKeyFile + "\"" : "")
                + (clientCertFile != null ? ", \"client_certificate_file\": \"" + clientCertFile + "\"" : "")
                + " }";
        return ByteSource.wrap(json.getBytes(StandardCharsets.UTF_8));
    }

    @Test
    public void testBasicConfig() throws Exception {
        ByteSource json = makeJson("http://localhost:2379", null, null, null);
        runBasicTests(EtcdClusterConfig.fromJson(json));
    }

    @Test
    public void testBasicConfigNoScheme() throws Exception {
        ByteSource json = makeJson("localhost:2379", null, null, null);
        runBasicTests(EtcdClusterConfig.fromJson(json));
    }

    @Test
    public void testSslConfig() throws Exception {
        ByteSource json = makeJson("https://localhost:2360", EtcdTestSuite.serverCert, null, null);
        runBasicTests(EtcdClusterConfig.fromJson(json));
    }

    @Test
    public void testSslCaCertConfig() throws Exception {
        ByteSource json = makeJson("https://localhost:2362", EtcdTestSuite.serverCert,
                EtcdTestSuite.clientKey, EtcdTestSuite.clientCert);
        runBasicTests(EtcdClusterConfig.fromJson(json));
    }

    @Test(expected = StatusRuntimeException.class, timeout = 5000)
    public void testSslConfig_noCert() throws Exception { // should fail
        ByteSource json = makeJson("https://localhost:2360", null, null, null);
        runBasicTests(EtcdClusterConfig.fromJson(json));
    }

    @Test(expected = StatusRuntimeException.class, timeout = 5000)
    public void testSslCaCertConfig_noClientCert() throws Exception { // should fail
        ByteSource json = makeJson("https://localhost:2362", EtcdTestSuite.serverCert, null, null);
        runBasicTests(EtcdClusterConfig.fromJson(json));
    }

    void runBasicTests(EtcdClusterConfig config) throws Exception {
        try (EtcdClient client = config.getClient()) {
            runBasicTests(client);
        }
    }

    void runBasicTests(KvStoreClient client) {
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
    }
}
