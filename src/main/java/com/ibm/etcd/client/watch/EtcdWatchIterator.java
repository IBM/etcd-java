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

import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import com.ibm.etcd.api.Event;
import com.ibm.etcd.api.ResponseHeader;
import com.ibm.etcd.client.kv.KvClient.Watch;
import com.ibm.etcd.client.kv.KvClient.WatchIterator;
import com.ibm.etcd.client.kv.WatchUpdate;

import io.grpc.stub.StreamObserver;

/**
 * Converts async observer-based watch to sync iterator-based watch
 */
class EtcdWatchIterator implements WatchIterator, StreamObserver<WatchUpdate> {
    
    static class CompletedUpdate implements WatchUpdate {
        final RuntimeException error;
        @Override public ResponseHeader getHeader() { return null; }
        @Override public boolean isFragment() { return false; }
        @Override public List<Event> getEvents() { return Collections.emptyList(); }
        @Override public String toString() { return "watch complete"; }
        public CompletedUpdate(RuntimeException error) {
            this.error = error;
        }
    }

    final BlockingQueue<WatchUpdate> updateQueue = new LinkedBlockingQueue<>();
    Watch watch;
    
    CompletedUpdate complete;
    
    EtcdWatchIterator setWatch(Watch watch) {
        this.watch = watch;
        return this;
    }
    
    @Override
    public boolean hasNext() {
        if(complete == null) {
            WatchUpdate wu = updateQueue.peek();
            if(!(wu instanceof CompletedUpdate) // includes null
                    || ((CompletedUpdate)wu).error != null) {
                return true;
            }
            updateQueue.remove();
            complete = (CompletedUpdate) wu;
        }
        return false;
    }

    @Override
    public WatchUpdate next() {
        if(complete == null) try {
            WatchUpdate wu = updateQueue.take();
            if(!(wu instanceof CompletedUpdate)) return wu;
            complete = (CompletedUpdate) wu;
            if(complete.error == null) return complete;
        } catch(InterruptedException ie) {
            throw new RuntimeException(ie);
        }
        throw complete.error != null ? complete.error
                : new NoSuchElementException();
    }

    @Override
    public void close() {
        if(watch == null) throw new IllegalStateException();
        watch.close();
    }

    @Override
    public void onNext(WatchUpdate value) {
        updateQueue.add(value);
    }
    @Override
    public void onError(Throwable t) {
        RuntimeException err = t instanceof RuntimeException
                ? (RuntimeException) t : new RuntimeException(t);
        updateQueue.add(new CompletedUpdate(err));
    }
    @Override
    public void onCompleted() {
        updateQueue.add(new CompletedUpdate(null));
    }
}


