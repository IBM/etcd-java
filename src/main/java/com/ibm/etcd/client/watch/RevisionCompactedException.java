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

import com.ibm.etcd.api.ResponseHeader;

/**
 * Indicates watch failure due to a required past revision having been compacted
 */
public class RevisionCompactedException extends WatchCancelledException {

    private static final long serialVersionUID = 3831051484800564889L;
    
    protected final long compactRevision;

    RevisionCompactedException(ResponseHeader header, String reason, long compactRevision) {
        super("Watch revision has been compacted, oldest available is "+compactRevision, header, reason);
        this.compactRevision = compactRevision;
    }
    
    /**
     * @return oldest available (non-compacted) revision
     */
    public long getCompactRevision() {
        return compactRevision;
    }
}
