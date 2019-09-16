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

import com.google.common.base.Strings;
import com.ibm.etcd.api.ResponseHeader;

/**
 * Indicates the watch was cancelled by the server
 */
public class WatchCancelledException extends RuntimeException {

    private static final long serialVersionUID = 6922272835138905911L;

    protected final ResponseHeader header;
    protected final long watchId;
    protected final String reason;

    WatchCancelledException(String message, ResponseHeader header, long watchId, String reason) {
        super(message == null ? reason : (Strings.isNullOrEmpty(reason)
                ? message : (message + "; " + reason)) + " (watchId = " + watchId + ")");
        this.header = header;
        this.watchId = watchId;
        this.reason = reason;
    }

    WatchCancelledException(ResponseHeader header, long watchId, String reason) {
        this("Watch was cancelled by the server unexpectedly", header, watchId, reason);
    }

    /**
     * @return the etcd response header returned with the cancellation response
     */
    public ResponseHeader getHeader() {
        return header;
    }

    /**
     * @return the reason for the cancellation, if provided
     */
    public String getReason() {
        return reason;
    }

    /**
     * @return the internal id of the cancelled watch
     */
    public long getWatchId() {
        return watchId;
    }
}
