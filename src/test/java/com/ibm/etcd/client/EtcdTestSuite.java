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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
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
@SuiteClasses({
    JsonConfigTest.class,
    ClientBuilderTest.class,
    KvTest.class,
    WatchTest.class,
    LeaseTest.class,
    LockTest.class,
    PersistentLeaseKeyTest.class,
    RangeCacheTest.class
    })
public class EtcdTestSuite {

    static Process etcdProcess, etcdTlsProcess, etcdTlsCaProcess, etcdJwtProcess;

    static final String etcdCommand;
    static final String etcdctlCommand;
    static {
         String etcd = System.getenv("ETCD_CMD");
         etcdCommand = etcd != null ? etcd : "etcd";
         String etcdctl = System.getenv("ETCDCTL_CMD");
         etcdctlCommand = etcdctl != null ? etcdctl : "etcdctl";
    }

    static final String clientKey = EtcdTestSuite.class.getResource("/client.key").getFile();
    static final String clientCert = EtcdTestSuite.class.getResource("/client.crt").getFile();
    static final String serverKey = EtcdTestSuite.class.getResource("/server.key").getFile();
    static final String serverCert = EtcdTestSuite.class.getResource("/server.crt").getFile();
    static final String jwtKey = EtcdTestSuite.class.getResource("/jwt_RS256.key").getFile(); // openssl genrsa -out jwt_RS256.key 4096
    static final String jwtPub = EtcdTestSuite.class.getResource("/jwt_RS256.pub").getFile(); // openssl rsa -in jwt_RS256.key -pubout > jwt_RS256.pub

    static final String userName = "root";
    static final String userPwd = UUID.randomUUID().toString();

    @BeforeClass
    public static void setUp() throws Exception {
        etcdProcess = startProcess();

        etcdTlsProcess = startProcess("--cert-file=" + serverCert,
                "--key-file=" + serverKey, "--listen-client-urls=https://localhost:2360",
                "--listen-peer-urls=http://localhost:2361",
                "--advertise-client-urls=https://localhost:2360", "--name=tls");

        etcdTlsCaProcess = startProcess("--cert-file=" + serverCert,
                "--key-file=" + serverKey, "--listen-client-urls=https://localhost:2362",
                "--listen-peer-urls=http://localhost:2363",
                "--advertise-client-urls=https://localhost:2362", "--name=tls-ca", 
                "--trusted-ca-file=" + clientCert, "--client-cert-auth");

        Path tmpDir = Files.createTempDirectory(null);
        tmpDir.toFile().deleteOnExit();
        etcdJwtProcess = startProcess("--auth-token=jwt,pub-key=" + jwtPub + ",priv-key=" + jwtKey + ",sign-method=RS256,ttl=4s",
                "--listen-client-urls=http://localhost:2365",
                "--listen-peer-urls=http://localhost:2364",
                "--advertise-client-urls=http://localhost:2365",
                "--data-dir=" + tmpDir);
        executeCtlCommand("--endpoints=localhost:2365",
                "user", "add", userName, "--new-user-password", userPwd);
        executeCtlCommand("--endpoints=localhost:2365",
                "user", "grant-role", userName, "root");
        executeCtlCommand("--endpoints=localhost:2365",
                "auth", "enable");
    }

    private static Process startProcess(String... cmdline) throws Exception {
        boolean ok = false;
        try {
            List<String> cmd = new ArrayList<>();
            cmd.add(etcdCommand);
            cmd.addAll(Arrays.asList(cmdline));
            Process etcdProcess = new ProcessBuilder(cmd)
                    .redirectErrorStream(true).start();
            waitForStartup(etcdProcess);
            ok = true;
            return etcdProcess;
        } catch (IOException e) {
            System.out.println("Failed to start etcd: " + e);
            return null;
        } finally {
            if (!ok) {
                tearDown(etcdProcess);
            }
        }
    }

    private static void executeCtlCommand(String... cmdline) throws Exception {
        boolean ok = false;
        Process p = null;
        try {
            List<String> cmd = new ArrayList<>();
            cmd.add(etcdctlCommand);
            cmd.addAll(Arrays.asList(cmdline));
            p = new ProcessBuilder(cmd)
                    .redirectErrorStream(true).start();
            waitForStartup(p);
            p.waitFor(30L, TimeUnit.SECONDS);
            System.out.println("etcdctl exit value:" + p.exitValue());
            ok = true;
        } catch (IOException e) {
            System.out.println("Failed to execute etcdctl: " + e);
        } finally {
            if (!ok) {
                tearDown(p);
            }
        }
    }

    @AfterClass
    public static void tearDown() {
        tearDown(etcdProcess);
        tearDown(etcdTlsProcess);
        tearDown(etcdTlsCaProcess);
        tearDown(etcdJwtProcess);
    }

    public static void tearDown(Process process) {
        if (process != null) {
            process.destroy();
        }
    }

    static void waitForStartup(Process process) throws Exception {
        if (process == null) {
            return;
        }
        ExecutorService es = Executors.newSingleThreadExecutor();
        TimeLimiter tl = SimpleTimeLimiter.create(es);
        try {
            tl.callWithTimeout(() -> {
                Reader isr = new InputStreamReader(process.getInputStream());
                BufferedReader br = new BufferedReader(isr);
                String line;
                while ((line = br.readLine()) != null &&
                        !line.contains("ready to serve client requests")) {
                    System.out.println(line);
                }
                return null;
            }, 10L, TimeUnit.SECONDS);
        } finally {
            es.shutdown();
        }
    }
}
