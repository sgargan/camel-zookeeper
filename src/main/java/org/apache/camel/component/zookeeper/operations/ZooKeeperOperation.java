/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.camel.component.zookeeper.operations;

import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.ZooKeeper;

/**
 * <code>ZooKeeperOperation</code> is the base class for wrapping various
 * ZooKeeper API instructions and callbacks into callable and composable operation
 * objects.
 */
@SuppressWarnings("unchecked")
public abstract class ZooKeeperOperation<ResultType> {

    protected final transient Log log = LogFactory.getLog(getClass());

    protected String node;

    protected ZooKeeper connection;

    private boolean producesExchange;

    protected Set<Thread> waitingThreads = new CopyOnWriteArraySet<Thread>();

    private boolean cancelled;

    protected OperationResult<ResultType> result;

    public ZooKeeperOperation(ZooKeeper connection, String node) {
        this(connection, node, true);
    }

    public ZooKeeperOperation(ZooKeeper connection, String node, boolean producesExchange) {
        this.connection = connection;
        this.node = node;
        this.producesExchange = producesExchange;
    }

    /**
     * Gets the result of this zookeeper operation, i.e. some data and the
     * associated node stats
     *
     * @return
     */
    public abstract OperationResult<ResultType> getResult();

    public OperationResult<ResultType> get() throws InterruptedException, ExecutionException {
        waitingThreads.add(Thread.currentThread());
        result = getResult();
        return result;
    }

    public OperationResult<ResultType> get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        return get(); // TODO ; perhaps set a timer here ....
    }

    public boolean cancel(boolean mayInterruptIfRunning) {
        if (mayInterruptIfRunning) {
            for (Thread waiting : waitingThreads) {
                waiting.interrupt();
            }
            cancelled = true;
        }
        return mayInterruptIfRunning;
    }

    public boolean isCancelled() {
        return cancelled;
    }

    public boolean isDone() {
        return result != null;
    }

    public String getNode() {
        return node;
    }

    public boolean shouldProduceExchange() {
        return producesExchange;
    }

    protected final static Class[] constructorArgs = {ZooKeeper.class, String.class};

    // TODO: slightly different to a clone as it uses the constructor
    public ZooKeeperOperation createCopy() throws Exception {
        return getClass().getConstructor(constructorArgs).newInstance(new Object[] {connection, node});
    }
}
