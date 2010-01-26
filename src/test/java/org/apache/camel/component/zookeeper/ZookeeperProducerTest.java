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
                from("direct:async-set").to(zookeeperUri);

            }
        }, new RouteBuilder() {
            public void configure() throws Exception {
                zookeeperUri = "zoo://localhost:39913/node?create=true";
                from("direct:sync-set"). to(zookeeperUri).to("mock:producer-out");
                from("zoo://localhost:39913/node?create=true").to("mock:consumed-from-node");
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

        mock.await(5, TimeUnit.SECONDS);
        mock.assertIsSatisfied();
        pipeline.assertIsSatisfied();
    }

}
