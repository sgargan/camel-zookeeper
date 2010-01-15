package org.apache.camel.component.zookeeper.operations;

/**
 * <code>ExchangelessOperation</code> is a wrapper for an Operation that will cause no exchange to be generated
 *
 * @author sgargan
 */
@SuppressWarnings("all")
public class ExchangelessOperation extends ZooKeeperOperation{

    private ZooKeeperOperation delegate;

    public ExchangelessOperation(ZooKeeperOperation delegate) {
        super(delegate.connection, delegate.node);
        this.delegate = delegate;
    }

    @Override
    public OperationResult getResult() {
        return delegate.getResult();
    }
}
