package org.apache.camel.component.zookeeper.policy;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.camel.Consumer;
import org.apache.camel.Exchange;
import org.apache.camel.Route;
import org.apache.camel.component.zookeeper.ZooKeeperComponent;
import org.apache.camel.impl.RoutePolicySupport;

/**
 * <code>ZooKeeperRoutePolicy</code> uses the leader election capabilities of a
 * ZooKeeper cluster to control how routes are enabled. It is typically used in
 * fail-over scenarios controlling identical instances of a route across a
 * cluster of Camel based servers.
 * <p>
 * The policy is configured with a 'top n' number of routes that should be
 * allowed to start, for a master/slave scenario this would be 1. Each instance
 * of the policy will execute the election algorithm to obtain its position in
 * the hierarchy of servers, if it is within the 'top n' servers then the policy
 * is enabled and exchanges can be processed by the route. If not it waits for a
 * change in the leader hierarchy and then reruns this scenario to see if it is
 * now in the top n.
 * <p>
 * All instances of the policy must also be configured with the same path on the
 * ZooKeeper cluster where the election will be carried out. It is good practice
 * for this to indicate the application e.g. /someapplication/someroute/
 * <p>
 * Check @link{ http://hadoop.apache
 * .org/zookeeper/docs/current/recipes.html#sc_leaderElection} for more on how
 * Leader election is achieved with ZooKeeper.
 *
 * @author sgargan
 */
public class ZooKeeperRoutePolicy extends RoutePolicySupport {

    private String electionNode;
    private int enabledCount;

    private final Lock lock = new ReentrantLock();

    private AtomicBoolean shouldProcessExchanges = new AtomicBoolean();

    public ZooKeeperRoutePolicy() {
        // TODO Auto-generated constructor stub
    }

    public ZooKeeperRoutePolicy(ZooKeeperComponent component, String electionNode, int enabledCount)
    {
        this.electionNode = electionNode;
        this.enabledCount = enabledCount;
    }

    @Override
    public void onExchangeBegin(Route route, Exchange exchange) {
        if(!shouldProcessExchanges.get())
        {
            try {
                lock.lock();
                stopConsumer(route.getConsumer());

            } catch (Exception e) {
                handleException(e);
            } finally {
                lock.unlock();
            }
            throw new IllegalStateException("Failing the exchange");
        }
        else
        {
            try {
                lock.lock();
                startConsumer(route.getConsumer());
            } catch (Exception e) {
                handleException(e);
            } finally {
                lock.unlock();
            }
        }
    }



    @Override
    protected boolean startConsumer(Consumer consumer) throws Exception {
        System.err.println("Starting consumer" + consumer);
        return super.startConsumer(consumer);
    }

    @Override
    protected boolean stopConsumer(Consumer consumer) throws Exception {
        System.err.println("Stopping consumer" + consumer);
        return super.stopConsumer(consumer);
    }


}
