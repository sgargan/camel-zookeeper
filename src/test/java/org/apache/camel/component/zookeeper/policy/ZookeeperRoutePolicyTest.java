package org.apache.camel.component.zookeeper.policy;

import java.util.concurrent.TimeUnit;

import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.mock.MockEndpoint;
import org.apache.camel.component.zookeeper.policy.ZooKeeperRoutePolicy;
import org.apache.camel.test.junit4.CamelTestSupport;
import org.junit.Test;

public class ZookeeperRoutePolicyTest extends CamelTestSupport{



    @Override
    protected RouteBuilder createRouteBuilder() throws Exception {
        return new RouteBuilder() {

            @Override
            public void configure() throws Exception {
                ZooKeeperRoutePolicy policy = new ZooKeeperRoutePolicy();
                from("direct:policy-controlled").routePolicy(policy).to("mock:controlled");
            }
        };
    }

    @Test
    public void routeCanBeDisabled() throws Exception {
        MockEndpoint mock = getMockEndpoint("mock:controlled");
        mock.setExpectedMessageCount(0);
        sendBody("direct:policy-controlled", "This is a test");
        mock.await(1, TimeUnit.SECONDS);
        mock.assertIsSatisfied();

    }
}
