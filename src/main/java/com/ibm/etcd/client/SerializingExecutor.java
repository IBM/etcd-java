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

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Delegates submitted tasks to the shared threadpool but ensures they
 * are executed in order and in serial
 * 
 */
public class SerializingExecutor implements Executor {
    
    private static final Logger logger = LoggerFactory.getLogger(SerializingExecutor.class);
    
	private final Executor sharedPool;
	private final Queue<Runnable> workQueue;
	private volatile boolean scheduled = false;
	
	public SerializingExecutor(Executor parentPool) {
	    this(parentPool, 0);
	}
	
	public SerializingExecutor(Executor parentPool, int capacity) {
        if(parentPool == null) throw new NullPointerException();
        this.sharedPool = parentPool;
        this.workQueue = capacity > 0 ? new LinkedBlockingQueue<>(capacity)
                : new ConcurrentLinkedQueue<>();
    }
	
	protected void logTaskUncheckedException(Throwable t) {
		logger.error("Uncaught task exception: "+t, t);
	}
	
	@SuppressWarnings("serial")
    class TaskRun extends ReentrantLock implements Runnable {
	    @Override public void run() {
            try {
                for(;;) {
                    Queue<Runnable> wq = workQueue;
                    Runnable next;
                    if((next = wq.poll()) == null) {
                        lock();
                        try {
                            scheduled = false;
                            if((next = wq.poll()) == null) return;
                            scheduled = true;
                        } finally {
                            unlock();
                        }
                    }
                    try {
                        next.run();
                    } catch(RuntimeException e) {
                        logTaskUncheckedException(e);
                    }
                }
            } catch(Throwable t) {
                dispatch();
                logTaskUncheckedException(t);
                throw t;
            }
        }
	}
	
	private final TaskRun runner = new TaskRun();
	
	@Override
	public void execute(Runnable command) {
	    if(!workQueue.offer(command)) {
	        throw new RejectedExecutionException("SerializingExecutor work queue full");
	    }
	        
		if(!scheduled) {
			boolean doit = false;
			runner.lock();
			try {
				if(!scheduled) {
					scheduled = true;
					doit = true;
				}
			} finally {
			    runner.unlock();
			}
			if(doit) dispatch();
		}
	}
	
	private void dispatch() {
		boolean ok = false;
		try {
			sharedPool.execute(runner);
			ok = true;
		} finally {
			if(!ok) {
			    runner.lock();
			    try {
				scheduled = false; // bad situation
			    } finally {
			        runner.unlock();
			    }
			}
		}
	}

}
