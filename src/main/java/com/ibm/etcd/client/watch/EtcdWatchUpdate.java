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
package com.ibm.etcd.client.watch;

import java.util.List;
import java.util.stream.Collectors;

import com.google.protobuf.ByteString;
import com.ibm.etcd.client.kv.WatchUpdate;
import com.ibm.etcd.api.Event;
import com.ibm.etcd.api.KeyValue;
import com.ibm.etcd.api.ResponseHeader;
import com.ibm.etcd.api.WatchResponse;

/**
 * @see WatchUpdate
 */
public class EtcdWatchUpdate implements WatchUpdate {

    private final WatchResponse response;

    EtcdWatchUpdate(WatchResponse response) {
        this.response = response;
    }

    public ResponseHeader getHeader() {
        return response.getHeader();
    }

    public List<Event> getEvents() {
        return response.getEventsList();
    }

    @Override
    public String toString() {
        return "WatchUpdate[rev="+response.getHeader().getRevision()
                +",events=["+getEvents().stream().map(e ->
                "Event[type="+e.getType()+",key="+kvToString(e.getKv())
                +",modRev="+(e.getKv()!=null?e.getKv().getModRevision():"n/a")
                +",prevKey="+kvToString(e.getPrevKv())+"]")
                .collect(Collectors.joining(",")) +"]]";
    }
    
    private static String kvToString(KeyValue kv) {
        if(kv == null) return null;
        ByteString key = kv.getKey();
        return key != null ? key.toStringUtf8() : "null";
    }
}
