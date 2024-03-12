package com.ibm.etcd.client.maintenance;

import com.google.common.util.concurrent.ListenableFuture;
import com.ibm.etcd.api.HashKVResponse;
import com.ibm.etcd.api.StatusResponse;
import com.ibm.etcd.api.DefragmentResponse;

public interface MaintenanceClient {
    /**
     *
     * @return future for {@link StatusResponse}
     */
    ListenableFuture<StatusResponse> status();

    /**
     *
     * @return future for {@link HashKVResponse}
     */
    ListenableFuture<HashKVResponse> hashkv();

    /**
     *
     * @return future for {@link DefragmentResponse}
     */
    ListenableFuture<DefragmentResponse> defrag();
}
