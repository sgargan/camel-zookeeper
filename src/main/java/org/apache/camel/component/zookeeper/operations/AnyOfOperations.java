package org.apache.camel.component.zookeeper.operations;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * <code>AnyOfOperations</code> is a composite operation of one or more sub
 * operation, executing each in turn until any one succeeds. If any execute
 * successfully, this operation succeeds; if the sub operations are all executed
 * without success it fails.
 * <p>
 * It is mostly used for test and watch scenarios where a node is tested for
 * existence, data or children, falling back to a corresponding watch operation
 * if the test operation fails.
 */
@SuppressWarnings("unchecked")
public class AnyOfOperations extends ZooKeeperOperation {

    private ZooKeeperOperation[] keeperOperations;

    public AnyOfOperations(String node, ZooKeeperOperation... keeperOperations) {
        super(null, node, false);
        this.keeperOperations = keeperOperations;
    }

    @Override
    public OperationResult get() throws InterruptedException, ExecutionException {
        for (ZooKeeperOperation op : keeperOperations) {
            try {
                OperationResult result = op.get();
                if (result.isOk()) {
                    return result;
                }
            } catch (Exception e) {
            }
        }
        throw new ExecutionException("All operations exhausted without one result", null);
    }

    @Override
    public OperationResult get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        return get();
    }

    @Override
    public OperationResult getResult() {
        return null;
    }

    @Override
    public ZooKeeperOperation createCopy() throws Exception {
        ZooKeeperOperation[] copy = new ZooKeeperOperation[keeperOperations.length];
        for (int x = 0; x < keeperOperations.length; x++) {
            copy[x] = keeperOperations[x].createCopy();
        }
        return new AnyOfOperations(node, copy);
    }
}
