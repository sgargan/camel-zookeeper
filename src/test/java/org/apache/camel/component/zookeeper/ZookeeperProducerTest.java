package org.apache.camel.component.zookeeper;

import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.mock.MockEndpoint;
import org.apache.camel.processor.interceptor.Tracer;
import org.junit.Test;

public class ZookeeperProducerTest extends ZooKeeperTestSupport {

    private String datasetUri;
    private String zookeeperUri;

    @Override
    protected RouteBuilder createRouteBuilder() throws Exception {
        return new RouteBuilder() {

            @Override
            public void configure() throws Exception {
                getContext().addInterceptStrategy(new Tracer());
                zookeeperUri = "zoo://localhost:39913/node";

                from("direct:setnode").to(zookeeperUri);
                from(zookeeperUri).to("mock:result");
            }
        };
    }

    @Test
    public void testRoundtripOfDataToAndFromZnode() throws Exception {
        MockEndpoint mock = getMockEndpoint("mock:result");
        mock.expectedMessageCount(1);

        sendBody("direct:setnode", "TestPayload");
        mock.assertIsSatisfied();
    }

}
