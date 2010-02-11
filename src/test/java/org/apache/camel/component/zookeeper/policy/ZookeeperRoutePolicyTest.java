package org.apache.camel.component.zookeeper.policy;

import java.util.concurrent.TimeUnit;

import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.mock.MockEndpoint;
import org.apache.camel.component.zookeeper.ZooKeeperTestSupport;
import org.junit.Test;

public class ZookeeperRoutePolicyTest extends ZooKeeperTestSupport {

    protected RouteBuilder createRouteBuilder() throws Exception {
        // set up the parent used to control the election
        client.createPersistent("/someapp", "App node to contain policy election nodes...");
        client.createPersistent("/someapp/somepolicy", "Policy node used by route policy to control routes...");
        return new ZooKeeperPolicyEnforcedRoute();
    }

    @Test
    public void routeCanBeControlledByPolicy() throws Exception {
        MockEndpoint mock = getMockEndpoint("mock:controlled");
        mock.setExpectedMessageCount(1);
        sendBody("direct:policy-controlled", "This is a test");
        mock.await(5, TimeUnit.SECONDS);
        mock.assertIsSatisfied();
    }

    public static class ZooKeeperPolicyEnforcedRoute extends RouteBuilder {
        public void configure() throws Exception {
            ZooKeeperRoutePolicy policy = new ZooKeeperRoutePolicy("zoo:localhost:39913/someapp/somepolicy", 1);
            policy.setCamelContext(getContext());
            from("direct:policy-controlled").routePolicy(policy).to("mock:controlled");
        }
    };
}
