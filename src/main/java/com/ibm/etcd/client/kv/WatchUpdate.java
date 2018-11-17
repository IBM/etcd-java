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
package com.ibm.etcd.client.kv;

import java.util.List;

import com.ibm.etcd.api.Event;
import com.ibm.etcd.api.ResponseHeader;

/**
 * A watch update which may contain one or more KeyValue events
 * (puts/deletes). If events is empty, the update is communicating
 * a progression in the global revision of the store (i.e. notification
 * that all events with revision &lt;= getHeader().getRevision() have
 * already been seen).
 */
public interface WatchUpdate {

    ResponseHeader getHeader();
    
    boolean isFragment();

    List<Event> getEvents();
}
