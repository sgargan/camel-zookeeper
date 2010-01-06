package org.apache.camel.component.zookeeper.monitors;

import org.apache.zookeeper.data.Stat;

public class WatchFutureResult<ResultType> {
    private Stat statistics;
    private ResultType result;
    private Exception exception;

    public WatchFutureResult(ResultType result, Stat statistics) {
        this.result = result;
        this.statistics = statistics;
    }

    public WatchFutureResult(Exception exception) {
        this.exception = exception;
    }

    public Exception getException() {
        return exception;
    }

    public Stat getStatistics() {
        return statistics;
    }

    public ResultType getResult() {
        return result;
    }

    public boolean isOk() {
        return exception == null;
    }
}
