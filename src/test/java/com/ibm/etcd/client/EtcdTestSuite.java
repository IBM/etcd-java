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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.concurrent.TimeUnit;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

import com.google.common.util.concurrent.SimpleTimeLimiter;
import com.google.common.util.concurrent.TimeLimiter;
import com.ibm.etcd.client.utils.PersistentLeaseKeyTest;
import com.ibm.etcd.client.utils.RangeCacheTest;

@RunWith(Suite.class)
@SuiteClasses({KvTest.class, WatchTest.class, LeaseTest.class,
    PersistentLeaseKeyTest.class, RangeCacheTest.class})
public class EtcdTestSuite {

    static Process etcdProcess;
    
    @BeforeClass
    public static void setUp() throws Exception {
        boolean ok = false;
        try {
            etcdProcess = Runtime.getRuntime().exec("etcd");
            waitForStartup();
            ok = true;
        } catch(IOException e) {
            System.out.println("Failed to start etcd: "+e);
            //e.printStackTrace();
        } finally {
            if(!ok) tearDown();
        }
    }
 
    @AfterClass
    public static void tearDown() throws IOException {
        if(etcdProcess!=null) etcdProcess.destroy();
    }
    
    static void waitForStartup() throws Exception {
        if(etcdProcess == null) return;
        TimeLimiter tl = new SimpleTimeLimiter();
        tl.callWithTimeout(() -> {
            Reader isr = new InputStreamReader(etcdProcess.getErrorStream());
            BufferedReader br = new BufferedReader(isr);
            String line;
            while((line = br.readLine()) != null &&
                    !line.contains("ready to serve client requests")) {
                System.out.println(line);
            }
            return null;
        }, 10L, TimeUnit.SECONDS, true);
    }
    
}
