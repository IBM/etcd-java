package com.ibm.etcd.client;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.ibm.etcd.api.*;
import com.ibm.etcd.client.maintenance.MaintenanceClient;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class MaintenanceTest {
    static KvStoreClient client;

    static MaintenanceClient maintenanceClient;

    @BeforeClass
    public static void setup() {
        client = EtcdClient.forEndpoint("localhost", 2379)
                .withPlainText().build();
        maintenanceClient = client.getMaintenanceClient();
    }
    @AfterClass
    public static void teardown() throws Exception {
        if (client != null) {
            client.close();
        }
    }

    @Test
    public void testStatus() throws Exception {
        StatusResponse resp = maintenanceClient.status().get(1, SECONDS);
        assertNotNull(resp.getVersion());
        assertTrue(resp.getDbSize() > 0L);
        assertTrue(resp.getDbSizeInUse() > 0L);
        assertTrue(resp.hasHeader());
        assertTrue(resp.getHeader().getRevision() > 0L);
    }

    @Test
    public void testHashkv() throws Exception {
        HashKVResponse resp = maintenanceClient.hashkv().get(1, SECONDS);
        assertNotNull(resp);
        assertTrue(resp.getHash() != 0L);
        assertTrue(resp.hasHeader());
        assertTrue(resp.getHeader().getRevision() > 0L);
    }

    @Test
    public void testDefrag() throws Exception {
        DefragmentResponse resp = maintenanceClient.defrag().get(5, SECONDS);
        assertNotNull(resp);
    }
}
