package org.apache.camel.component.zookeeper.monitors;

import static org.junit.Assert.assertEquals;

import org.apache.camel.component.zookeeper.monitors.WatchFuture;
import org.apache.camel.component.zookeeper.monitors.WatchFutureResult;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.data.Stat;
import org.junit.Test;

public class WatchingFutureTest {
    private String data = "Event Received";
    private Stat statistics = new Stat();

    @Test
    public void shouldWaitForEvents() throws Exception {
        final WatchFuture<String> future = new WatchFuture<String>(null, "somepath", EventType.NodeCreated) {
            @Override
            protected WatchFutureResult<String> getResult(WatchedEvent event) {
                return new WatchFutureResult<String>(data, statistics);
            }
        };

        WatchedEvent event = new WatchedEvent(EventType.NodeCreated, null, "somepath");
        fireEventIn(future, event, 100);
        assertEquals(data, future.get().getResult());
        assertEquals(statistics, future.get().getStatistics());
        assertEquals(event, future.getEvent());
    }

    private void fireEventIn(final WatchFuture<String> future, final WatchedEvent event, final int millisecondsTillFire) {
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
