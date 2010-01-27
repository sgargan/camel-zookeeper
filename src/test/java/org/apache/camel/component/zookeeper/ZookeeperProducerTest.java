package org.apache.camel.component.zookeeper;

import java.util.concurrent.TimeUnit;

import org.apache.camel.Exchange;
import org.apache.camel.ExchangePattern;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.mock.MockEndpoint;
import org.junit.Test;

public class ZookeeperProducerTest extends ZooKeeperTestSupport {

    private String zookeeperUri;

    @Override
    protected RouteBuilder[] createRouteBuilders() throws Exception {
        return new RouteBuilder[] {new RouteBuilder() {
            public void configure() throws Exception {
                zookeeperUri = "zoo://localhost:39913/node?create=true";
                from("direct:sync-set"). to(zookeeperUri).to("mock:producer-out");
                from(zookeeperUri).to("mock:consumed-from-node");
            }
        }, new RouteBuilder() {
            public void configure() throws Exception {
                from("direct:no-create-fails-set").to("zoo://localhost:39913/doesnotexist");
            }
        },
        new RouteBuilder() {
            public void configure() throws Exception {
                from("direct:node-from-header").to("zoo://localhost:39913/notset?create=true");
                from("zoo://localhost:39913/set?create=true").to("mock:consumed-from-node");
            }
        }};
    }

    @Test
    public void testRoundtripOfDataToAndFromZnode() throws Exception {
        MockEndpoint mock = getMockEndpoint("mock:consumed-from-node");
        MockEndpoint pipeline = getMockEndpoint("mock:producer-out");
        mock.expectedMessageCount(1);
        pipeline.expectedMessageCount(1);

        Exchange e = createExchangeWithBody("TestPayload");
        e.setPattern(ExchangePattern.InOut);
        template.send("direct:sync-set", e);

        mock.await(2, TimeUnit.SECONDS);
        mock.assertIsSatisfied();
        pipeline.assertIsSatisfied();
    }

    @Test
    public void testAsyncRoundtripOfDataToAndFromZnode() throws Exception {
        MockEndpoint mock = getMockEndpoint("mock:consumed-from-node");
        mock.expectedMessageCount(1);

        sendBody("direct:sync-set", "TestPayload");

        mock.await(2, TimeUnit.SECONDS);
        mock.assertIsSatisfied();
    }

    @Test
    public void setUsingNodeFromHeader() throws Exception {
        MockEndpoint mock = getMockEndpoint("mock:consumed-from-node");
        mock.expectedMessageCount(1);

        Exchange e = createExchangeWithBody("TestPayload");
        e.setPattern(ExchangePattern.InOut);
        template.sendBodyAndHeader("direct:node-from-header", e, ZooKeeperMessage.ZOOKEEPER_NODE, "/set");

        mock.await(5, TimeUnit.SECONDS);
        mock.assertIsSatisfied();
    }

}
