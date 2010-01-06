package org.apache.camel.component.zookeeper.monitors;

import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher.Event.EventType;

public abstract class WatchFuture<ResultType> implements Future<WatchFutureResult<ResultType>>, Watcher {

    protected ZooKeeper connection;

    private EventType[] watchedForTypes;

    private Set<Thread> waitingThreads = new CopyOnWriteArraySet<Thread>();

    private CountDownLatch waitForAnyWatchedType = new CountDownLatch(1);

    private boolean cancelled;

    private WatchedEvent event;

    private String node;

    private WatchFutureResult<ResultType> result;

    public WatchFuture(ZooKeeper connection, String node, EventType... watchedForTypes) {
        this.connection = connection;
        this.node = node;
        this.watchedForTypes = watchedForTypes;
    }

    public void process(WatchedEvent event) {
        this.event = event;
        EventType received = event.getType();
        for (EventType watched : watchedForTypes) {
            if (watched.equals(received)) {
                result = getResult(event);
                waitForAnyWatchedType.countDown();
            }
        }
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

    protected abstract void beginNodeWatch() throws Exception;

    protected abstract WatchFutureResult<ResultType> getResult(WatchedEvent event);

    public WatchFutureResult<ResultType> get() throws InterruptedException, ExecutionException {
        waitForAnyWatchedType.await();
        return result;
    }

    public WatchFutureResult<ResultType> get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        waitForAnyWatchedType.await(timeout, unit);
        return result;
    }

    public boolean isCancelled() {
        return cancelled;
    }

    public boolean isDone() {
        return waitForAnyWatchedType.getCount() == 0;
    }

    public WatchedEvent getEvent() {
        return event;
    }

    public EventType[] getWatchedForTypes() {
        return watchedForTypes;
    }

    public String getNode() {
        return node;
    }

}
