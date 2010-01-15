package org.apache.camel.component.zookeeper.operations;

import static org.junit.Assert.assertEquals;

import org.apache.camel.component.zookeeper.operations.FutureEventDrivenOperation;
import org.apache.camel.component.zookeeper.operations.OperationResult;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.data.Stat;
import org.junit.Test;

public class FutureEventDrivenOperationTest {
    private String data = "Event Received";
    private Stat statistics = new Stat();

    @Test
    public void shouldWaitForEvents() throws Exception {
        final FutureEventDrivenOperation<String> future = new FutureEventDrivenOperation<String>(null, "somepath", EventType.NodeCreated) {

            @Override
            protected void installWatch() {
            }

            @Override
            public OperationResult<String> getResult() {
                return new OperationResult<String>(data, statistics);
            }
        };

        WatchedEvent event = new WatchedEvent(EventType.NodeCreated, null, "somepath");
        fireEventIn(future, event, 100);
        assertEquals(data, future.get().getResult());
        assertEquals(statistics, future.get().getStatistics());
        assertEquals(event, future.getEvent());
    }

    private void fireEventIn(final FutureEventDrivenOperation<String> future, final WatchedEvent event, final int millisecondsTillFire) {
        new Thread(new Runnable() {

            public void run() {

                try {
                    Thread.sleep(millisecondsTillFire);
                    future.process(event);
                } catch (InterruptedException e) {
                }
            }
        }).start();
    }
}
