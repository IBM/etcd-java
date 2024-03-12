package com.ibm.etcd.client.maintenance;

import com.google.common.util.concurrent.ListenableFuture;
import com.ibm.etcd.api.*;
import com.ibm.etcd.client.GrpcClient;
import io.grpc.MethodDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EtcdMaintenanceClient implements MaintenanceClient {
    private static final Logger logger = LoggerFactory.getLogger(EtcdMaintenanceClient.class);

    private static final MethodDescriptor<StatusRequest,StatusResponse> METHOD_ENDPOINT_STATUS =
            MaintenanceGrpc.getStatusMethod();
    private static final MethodDescriptor<HashKVRequest,HashKVResponse> METHOD_ENDPOINT_HASHKV =
            MaintenanceGrpc.getHashKVMethod();
    private static final MethodDescriptor<DefragmentRequest,DefragmentResponse> METHOD_ENDPOINT_DEFRAG =
            MaintenanceGrpc.getDefragmentMethod();

    private final GrpcClient client;

    public EtcdMaintenanceClient(GrpcClient client) {
        this.client = client;
    }

    @Override
    public ListenableFuture<StatusResponse> status() {
        return client.call(METHOD_ENDPOINT_STATUS, StatusRequest.newBuilder().build(), false);
    }

    @Override
    public ListenableFuture<HashKVResponse> hashkv() {
        return client.call(METHOD_ENDPOINT_HASHKV, HashKVRequest.newBuilder().build(), false);
    }

    @Override
    public ListenableFuture<DefragmentResponse> defrag() {
        return client.call(METHOD_ENDPOINT_DEFRAG, DefragmentRequest.newBuilder().build(), false);
    }

}
