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

import static com.ibm.etcd.client.KvTest.bs;
import static org.junit.Assert.*;

import org.junit.Test;

import com.google.protobuf.ByteString;
import com.ibm.etcd.client.EtcdClient;

public class LeaderElectionTest {

    @Test
    public void basicLeaderElectionTest() throws Exception {
        
        try(EtcdClient client1 = EtcdClient.forEndpoint("localhost", 2379)
                .withPlainText().build()) {
            
            ByteString electionKey = bs("/electiontest");

            EtcdLeaderElection observer = new EtcdLeaderElection(client1, electionKey);

            observer.start();

            assertNull(observer.getId());
            assertNull(observer.getLeaderId());
            
            EtcdLeaderElection alice = new EtcdLeaderElection(client1, electionKey, "alice");
            EtcdLeaderElection bob = new EtcdLeaderElection(client1, electionKey, "bob");
            EtcdLeaderElection claire = new EtcdLeaderElection(client1, electionKey, "claire");

            bob.start();
            
            Thread.sleep(500L);
            
            assertEquals("bob", bob.getId());
            assertEquals("bob", bob.getLeaderId());
            assertEquals("bob", observer.getLeaderId());
            
            alice.start();
            
            Thread.sleep(500L);
            
            assertEquals("alice", alice.getId());
            assertEquals("bob", observer.getLeaderId());
            assertEquals("bob", bob.getLeaderId());
            assertEquals("bob", alice.getLeaderId());
            assertTrue(bob.isLeader());
            assertFalse(alice.isLeader());
            
            claire.start();
            
            Thread.sleep(500L);
            assertEquals("bob", claire.getLeaderId());
            assertEquals("bob", observer.getLeaderId());
            bob.close();
            Thread.sleep(500L);
            assertEquals("alice", observer.getLeaderId());
            assertTrue(alice.isLeader());
            assertFalse(bob.isLeader());
            assertFalse(claire.isLeader());
            assertEquals("alice", claire.getLeaderId());
            
        }
    }
    
}
