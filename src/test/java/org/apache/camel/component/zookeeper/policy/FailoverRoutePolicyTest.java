package org.apache.camel.component.zookeeper.policy;

import java.util.concurrent.TimeUnit;

import org.apache.camel.CamelContext;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.mock.MockEndpoint;
import org.apache.camel.component.zookeeper.ZooKeeperTestSupport;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.impl.DefaultProducerTemplate;
import org.junit.Test;

public class FailoverRoutePolicyTest extends ZooKeeperTestSupport {

    protected CamelContext createCamelContext() throws Exception {
        disableJMX();
        // set up the parent used to control the election
        client.createPersistent("/someapp", "App node to contain policy election nodes...");
        client.createPersistent("/someapp/somepolicy", "Policy node used by route policy to control routes...");
        return super.createCamelContext();
    }

    @Test
    public void routeCanBeControlledByPolicy() throws Exception {
        ZookeeperPolicyEnforcedContext tetrisisMasterOfBlocks = createEnforcedContext("master");
        ZookeeperPolicyEnforcedContext slave = createEnforcedContext("slave");
        tetrisisMasterOfBlocks.sendMessageToEnforcedRoute("LIIIIIIIIIINNNNNNNNNEEEEEEE PEEEEEEICCCE", 1);
        slave.sendMessageToEnforcedRoute("But lord there is no place for a square!??!", 0);

        // trigger failover by killing the master...
        tetrisisMasterOfBlocks.shutdown();
        delay(10000);
        System.err.println("");
        System.err.println("");
        System.err.println("");
        System.err.println("");
        slave.sendMessageToEnforcedRoute("What a cruel and angry god...", 1);
    }

    private static class ZookeeperPolicyEnforcedContext {
        private CamelContext controlledContext;
        private ProducerTemplate template;
        private MockEndpoint mock;
        private String routename;

        public ZookeeperPolicyEnforcedContext(String name) throws Exception {
            controlledContext = new DefaultCamelContext();
            routename = name;
            template = new DefaultProducerTemplate(controlledContext);
            mock = controlledContext.getEndpoint("mock:controlled", MockEndpoint.class);
            controlledContext.addRoutes(new FailoverRoute(name));
            controlledContext.start();
        }

        public void sendMessageToEnforcedRoute(String message, int expected) throws InterruptedException {
            mock.expectedMessageCount(expected);
            try {
                template.requestBody("vm:"+routename, message);
            } catch (Exception e) {
                if (expected > 0) {
                    fail("Expected messages...");
                }
            }
            mock.await(2, TimeUnit.SECONDS);
            mock.assertIsSatisfied();
        }

        public void shutdown() throws Exception {
            controlledContext.stop();
        }
    }

    private ZookeeperPolicyEnforcedContext createEnforcedContext(String name) throws Exception, InterruptedException {
        ZookeeperPolicyEnforcedContext context = new ZookeeperPolicyEnforcedContext(name);
        delay(1000);
        return context;
    }

    public static class FailoverRoute extends RouteBuilder {

        private String routename;

        public FailoverRoute(String routename) {
            // need names as if we use the same direct ep name in two contexts
            // in the same vm shutting down one context shuts the endpoint for
            // both.
            this.routename = routename;
        }

        public void configure() throws Exception {
            ZooKeeperRoutePolicy policy = new ZooKeeperRoutePolicy("zoo:localhost:39913/someapp/somepolicy", 1);
            policy.setCamelContext(getContext());
            from("vm:" + routename).routePolicy(policy).id(routename).to("mock:controlled");
        }
    };
}
