package org.apache.camel.component.zookeeper.operations;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.data.Stat;

/**
 * <code>OperationResult</code> is used to ecapsulate the results of executing a
 * {@link ZooKeeperOperation}
 */
public class OperationResult<ResultType> {
    private Stat statistics;
    private ResultType result;
    private Exception exception;
    private boolean ok;

    public OperationResult(ResultType result, Stat statistics) {
        this(result, statistics, true);
    }

    public OperationResult(ResultType result, Stat statistics, boolean ok) {
        this.result = result;
        this.statistics = statistics;
        this.ok = ok;
    }

    public OperationResult(Exception exception) {
        this.exception = exception;
        ok = false;
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
        return ok;
    }

    public boolean failedDueTo(Code... codes) {
        if (exception instanceof KeeperException) {

            for (Code code : codes) {
                if (code.equals(((KeeperException)exception).code())) {
                    return true;
                }
            }
        }
        return false;
    }
}
