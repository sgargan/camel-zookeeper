package org.apache.camel.component.zookeeper.operations;

import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.log4j.Logger;
import org.apache.zookeeper.ZooKeeper;

/**
 * <code>ZooKeeperOperation</code>
 *
 * @author sgargan
 */
@SuppressWarnings("unchecked")
public abstract class ZooKeeperOperation<ResultType> {

    protected final Logger log = Logger.getLogger(getClass());

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

    public boolean shouldProduceExchange()
    {
        return producesExchange;
    }

    protected final static Class[] constructorArgs = {ZooKeeper.class, String.class};

    // TODO: slightly different to a clone as it uses the constructor
    public ZooKeeperOperation createCopy() throws Exception {
        return getClass().getConstructor(constructorArgs).newInstance(new Object[] {connection, node});
    }
}
