package org.apache.camel.component.zookeeper.operations;

import static java.lang.String.format;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher.Event.EventType;

/**
 * <code>FutureEventDrivenOperation</code> uses ZooKeepers {@link Watcher}
 * mechanism to await specific ZooKeeper events. Typically this is used to await changes
 * to a particular node before retrieving the change.
 *
 * @author sgargan
 */
public abstract class FutureEventDrivenOperation<ResultType> extends ZooKeeperOperation<ResultType> implements Watcher {

     private EventType[] awaitedTypes;

    private CountDownLatch waitForAnyWatchedType = new CountDownLatch(1);

    private WatchedEvent event;

    public FutureEventDrivenOperation(ZooKeeper connection, String node, EventType... awaitedTypes) {
        super(connection, node);
        this.awaitedTypes = awaitedTypes;
    }

    public void process(WatchedEvent event) {
        this.event = event;
        EventType received = event.getType();
        if (log.isDebugEnabled()) {
            log.debug(format("Recieved event of type %s for node '%s'", received, event.getPath()));
        }

        for (EventType watched : awaitedTypes) {
            if (watched.equals(received)) {
                result = getResult();
                waitForAnyWatchedType.countDown();
            }
        }

        if (log.isTraceEnabled() && waitForAnyWatchedType.getCount() > 0) {

            StringBuilder b = new StringBuilder();
            for (EventType type : awaitedTypes) {
                b.append(type).append(", ");
            }
            if(b.length() > 0) {
                b.setLength(b.length() - 2);
            }
            log.trace(String.format("Recieved event of type %s did not match any watched types %s", received, awaitedTypes));
        }
    }

    public OperationResult<ResultType> get() throws InterruptedException, ExecutionException {
        installWatch();
        waitingThreads.add(Thread.currentThread());
        waitForAnyWatchedType.await();
        return result;
    }

    public OperationResult<ResultType> get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        installWatch();
        waitingThreads.add(Thread.currentThread());
        waitForAnyWatchedType.await(timeout, unit);
        return result;
    }

    /**
     * Install the watcher to receive {@link WatchedEvent}s. It should use the
     * appropriate asynchronous ZooKeeper call to do this so as not to block the
     * route from starting. Once one of the watched for types of event is
     * received a call is made to getResult, which can use the appropriate
     * synchronous call to retrieve the actual data.
     */
    protected abstract void installWatch();

    public WatchedEvent getEvent() {
        return event;
    }

    public EventType[] getWatchedForTypes() {
        return awaitedTypes;
    }

}
