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

import java.util.concurrent.Executor;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.GeneratedMessageV3.Builder;
import com.ibm.etcd.client.kv.KvClient.RetryStrategy;

import io.grpc.Deadline;
import io.grpc.MethodDescriptor;

/**
 * Internal superinterface used for fluent API requests
 */
public interface FluentRequest<FR extends FluentRequest<FR,ReqT,RespT>,ReqT,RespT> {
    ListenableFuture<RespT> async();
    ListenableFuture<RespT> async(Executor executor);
    RespT sync();
    /**
     * timeout is <b>per attempt</b>
     */
    FR timeout(long millisecs);
    /**
     * deadline is absolute for entire request
     */
    FR deadline(Deadline deadline);
    FR backoffRetry();
    FR backoffRetry(Condition precondition);
    ReqT asRequest();


    @SuppressWarnings("unchecked")
    abstract class AbstractFluentRequest<FR extends FluentRequest<FR,ReqT,RespT>,ReqT,RespT,BldT
        extends Builder<BldT>> implements FluentRequest<FR,ReqT,RespT> {

        protected final GrpcClient client;
        protected final BldT builder;
        protected RetryStrategy retryStrategy = RetryStrategy.BASIC;
        protected Condition precondition;
        protected long timeoutMs;
        protected Deadline deadline;

        protected AbstractFluentRequest(GrpcClient client, BldT builder) {
            this.client = client;
            this.builder = builder;
        }

        protected abstract MethodDescriptor<ReqT,RespT> getMethod();
        protected abstract boolean idempotent();

        @Override
        public FR timeout(long millisecs) {
            this.timeoutMs = millisecs;
            return (FR) this;
        }
        @Override
        public FR deadline(Deadline deadline) {
            this.deadline = deadline;
            return (FR) this;
        }
        @Override
        public final FR backoffRetry() {
            this.retryStrategy = RetryStrategy.BACKOFF;
            return (FR) this;
        }
        @Override
        public final FR backoffRetry(Condition precondition) {
            this.retryStrategy = RetryStrategy.BACKOFF;
            this.precondition = precondition;
            return (FR) this;
        }
        @Override
        public final ReqT asRequest() {
            return (ReqT) builder.build();
        }
        @Override
        public ListenableFuture<RespT> async(Executor executor) {
            return client.call(getMethod(), precondition, (ReqT) builder.build(),
                    executor, GrpcClient.retryDecision(idempotent()), 
                    retryStrategy == RetryStrategy.BACKOFF, deadline, timeoutMs);
        }
        @Override
        public final ListenableFuture<RespT> async() {
            return async(null);
        }
        @Override
        public final RespT sync() {
            return client.waitForCall(this::async);
        }
    }
}
