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

import static com.ibm.etcd.client.KeyUtils.bs;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;
import com.ibm.etcd.client.EtcdClient;
import com.ibm.etcd.client.utils.RangeCache.Listener;
import com.ibm.etcd.api.KeyValue;

/**
 * Etcd-based leader election pattern
 * 
 */
public class EtcdLeaderElection implements AutoCloseable {

    protected static final Logger logger = LoggerFactory.getLogger(EtcdLeaderElection.class);

    protected final ByteString bytesPrefix;
    protected final int prefixLen;

    protected final String id;
    protected final RangeCache candidates;
    protected final PersistentLeaseKey ourCandidate; // null if id is null

    protected final List<ElectionListener> listeners = new CopyOnWriteArrayList<>();

    protected volatile String leaderId;
    protected volatile boolean leader; // us

    protected boolean initialized;


    /**
     * Observer-only mode, won't participate in election, leaders have no effect
     */
    public EtcdLeaderElection(EtcdClient client, ByteString prefix) {
        this(client, prefix, null);
    }

    // maybe use serializedexecutor here

    public EtcdLeaderElection(EtcdClient client, ByteString prefix, String candidateId) {
        if (candidateId != null && candidateId.contains("\\n")) {
            throw new IllegalArgumentException("id can't contain linebreak");
        }
        this.bytesPrefix = prefix;
        this.prefixLen = bytesPrefix.size();
        this.id = candidateId;
        this.candidates = new RangeCache(client, prefix, true);

        candidates.addListener((event,kv) -> {
            if (!initialized) {
                if (event != Listener.EventType.INITIALIZED) {
                    return;
                }
                initialized = true;
            }
            updateLeader();
        });
        ourCandidate = id == null ? null
                : new PersistentLeaseKey(client, cacheKey(id), bs(id), candidates);
    }

    private void updateLeader() {
        synchronized (candidates) {
            if (candidates.isClosed()) {
                return;
            }
            KeyValue chosen = null;
            for (KeyValue kv : candidates) {
                if (chosen == null || kv.getCreateRevision() < chosen.getCreateRevision()) {
                    chosen = kv;
                }
            }
            String chosenId = chosen == null ? null : candId(chosen);
            String priorLeaderId = leaderId;
            leaderId = chosenId;
            if (id == null) {
                return; // observer only
            }
            boolean wasUs = id.equals(priorLeaderId), isUs = id.equals(chosenId);
            if (wasUs ^ isUs) {
                leader = isUs;
                notifyListeners(isUs);
            }
        }
    }

    public String getId() {
        return id;
    }

    public String getLeaderId() {
        return leaderId;
    }

    public boolean isLeader() {
        return leader;
    }

    public void addListener(ElectionListener listener) {
        listeners.add(listener);
    }

    public boolean removeListener(ElectionListener listener) {
        return listeners.remove(listener);
    }

    //TODO custom executors ?
    protected void notifyListeners(boolean isLeader) {
        for (ElectionListener l : listeners) try {
            l.leadershipChange(isLeader);
        } catch (Exception e) {
            logger.warn("ElectionListener threw exception", e);
        }
    }

    public void start() {
        synchronized (candidates) {
            candidates.start();
            if (ourCandidate != null) {
                ourCandidate.start();
            }
        }
    }

    @Override
    public synchronized void close() {
        synchronized (candidates) {
            if (ourCandidate != null) {
                ourCandidate.close();
            }
            candidates.close();
            leader = false;
            leaderId = null;
        }
    }

    // ------ static conversion utility methods

    protected ByteString cacheKey(String str) {
        return str == null ? null : bytesPrefix.concat(bs(str));
    }

    protected static String candId(KeyValue kv) {
        String str = kv.getValue().toStringUtf8();
        int nl = str.indexOf('\n');
        return nl == -1 ? str : str.substring(0, nl);
    }

    // ------ election listener class

    public interface ElectionListener {
        /**
         * Called if our leadership status changes
         * 
         * @param isLeader true if we are the leader, false otherwise
         */
        void leadershipChange(boolean isLeader);
    }

}
