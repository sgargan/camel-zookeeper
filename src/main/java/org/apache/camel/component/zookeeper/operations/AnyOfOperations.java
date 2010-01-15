package org.apache.camel.component.zookeeper.operations;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@SuppressWarnings("unchecked")
public class AnyOfOperations extends ZooKeeperOperation{

    private ZooKeeperOperation[] keeperOperations;

    public AnyOfOperations(String node, ZooKeeperOperation... keeperOperations )
    {
        super(null, node, false);
        this.keeperOperations = keeperOperations;
    }

    @Override
    public OperationResult get() throws InterruptedException, ExecutionException {
        for(ZooKeeperOperation op: keeperOperations) {
            try {
                OperationResult result = op.get();
                if(result.isOk()) {
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
