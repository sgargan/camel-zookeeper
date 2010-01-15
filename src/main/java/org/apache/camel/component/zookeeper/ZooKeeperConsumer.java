package org.apache.camel.component.zookeeper;

import static java.lang.String.format;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.component.zookeeper.operations.AnyOfOperations;
import org.apache.camel.component.zookeeper.operations.ChildrenChangedOperation;
import org.apache.camel.component.zookeeper.operations.DataChangedOperation;
import org.apache.camel.component.zookeeper.operations.ExistenceChangedOperation;
import org.apache.camel.component.zookeeper.operations.ExistsOperation;
import org.apache.camel.component.zookeeper.operations.GetDataOperation;
import org.apache.camel.component.zookeeper.operations.OperationResult;
import org.apache.camel.component.zookeeper.operations.ZooKeeperOperation;
import org.apache.camel.impl.DefaultConsumer;
import org.apache.camel.util.concurrent.ExecutorServiceHelper;
import org.apache.zookeeper.ZooKeeper;

@SuppressWarnings("unchecked")
public class ZooKeeperConsumer extends DefaultConsumer {

    private ZooKeeperConnectionManager connectionManager;

    private ZooKeeper connection;

    private ZooKeeperConfiguration configuration;

    private LinkedBlockingQueue<ZooKeeperOperation> operations = new LinkedBlockingQueue<ZooKeeperOperation>();

    private boolean shuttingDown;

    private ExecutorService executor;

    public ZooKeeperConsumer(ZooKeeperEndpoint endpoint, Processor processor) {
        super(endpoint, processor);
        this.connectionManager = endpoint.getConnectionManager();
        this.configuration = endpoint.getConfiguration();
    }

    @Override
    protected void doStart() throws Exception {
        super.doStart();
        connection = connectionManager.getConnection();
        if (log.isDebugEnabled()) {
            log.debug(format("Connected to Zookeeper cluster %s", configuration.getConnectString()));
        }

        initializeConsumer();
        executor = ExecutorServiceHelper.newFixedThreadPool(1, configuration.getPath() + "-OperationsService", true);
        ConsumerService OpsService = new ConsumerService();
        executor.execute(OpsService);
    }

    @Override
    protected void doStop() throws Exception {
        super.doStop();
        shuttingDown = true;
        connection = connectionManager.getConnection();
        if (log.isTraceEnabled()) {
            log.trace(format("Shutting down zookeeper consumer of '%s'", configuration.getPath()));
        }
        executor.shutdown();
        connectionManager.shutdown();
    }


    private void initializeConsumer() {
        String node = configuration.getPath();
        if (configuration.listChildren()) {
            initializeChildListingConsumer(node);
        } else {
            initializeDataConsumer(node);
        }
    }

    private void initializeDataConsumer(String node) {
        if (!shuttingDown) {
            if (log.isDebugEnabled()) {
                log.debug(String.format("Initailizing data watch on node '%s'", node));
            }
           addBasicDataConsumeSequence(node);
        }
    }

    private void initializeChildListingConsumer(String node) {
        if (!shuttingDown) {
            if (log.isDebugEnabled()) {
                log.debug(String.format("Initailizing child listing watch on node '%s'", node));
            }
            try {
                ChildrenChangedOperation op = new ChildrenChangedOperation(connection, node);
                OperationResult result = op.get();
                getProcessor().process(createExchange(node, result));

            } catch (Exception ex) {
                handleException(ex);

            } finally {
                if (configuration.shouldRepeat()) {
                    if (log.isDebugEnabled()) {
                        log.debug(String.format("Reinstating watch on '%s'", node));
                    }
                    initializeChildListingConsumer(node);
                }
            }
        }
    }

    private Exchange createExchange(String path, OperationResult result) {
        Exchange e = getEndpoint().createExchange();
        ZooKeeperMessage in = new ZooKeeperMessage(path, result.getStatistics());
        e.setIn(in);
        System.err.println("Creating exchange with "+result.getResult());
        if (result.isOk()) {
            in.setBody(result.getResult());
        } else {
            e.setException(result.getException());
        }
        return e;
    }

    private class ConsumerService implements Runnable {

        private ZooKeeperOperation current = null;

        public void run() {
            while (isRunAllowed()) {

                try {
                    current = operations.take();
                    log.warn(current.getClass().getSimpleName());
                } catch (InterruptedException e) {
                    continue;
                }
                String node = current.getNode();
                try {
                    OperationResult result = current.get();
                    if (result != null && current.shouldProduceExchange()) {
                        getProcessor().process(createExchange(node, result));
                    }
                } catch (Exception e) {
                    handleException(e);
                    backoffAndThenRestart();
                } finally {
                    if (configuration.shouldRepeat()) {
                        try {
                            System.err.println("Reinstating"+current.getClass().getSimpleName());
                            operations.offer(current.createCopy());
                        } catch (Exception e) {
                            e.printStackTrace();
                            backoffAndThenRestart();
                        }
                    }
                }
            }
        }

        private void backoffAndThenRestart() {
            try {
                if (isRunAllowed()) {
                    Thread.sleep(5000);
                    initializeConsumer();
                }
            } catch (Exception e) {
            }
        }
    }

    private void addBasicDataConsumeSequence(String node){
        operations.clear();
        operations.add(new AnyOfOperations(node, new ExistsOperation(connection, node), new ExistenceChangedOperation(connection, node)));
        operations.add(new GetDataOperation(connection, node));
        operations.add(new DataChangedOperation(connection, node, false));
    }
}
