package org.apache.camel.component.zookeeper;

import java.util.concurrent.TimeUnit;

import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.mock.MockEndpoint;
import org.junit.Test;

public class ConsumeDataTest extends ZooKeeperTestSupport {

    @Override
    protected RouteBuilder createRouteBuilder() throws Exception {
        return new RouteBuilder() {
            @Override
            public void configure() throws Exception {
                from("zoo://localhost:39913/camel").to("mock:zookeeper-data");
            }
        };
    }

    @Test
    public void shouldGetDataNotification() throws Exception {
        String testPayload = "This is a test";
        MockEndpoint mock = getMockEndpoint("mock:zookeeper-data");
        mock.expectedBodiesReceived(testPayload);
        addTestData("/camel", testPayload);
        mock.await(5, TimeUnit.SECONDS);
        mock.assertIsSatisfied();

    }

    private void addTestData(String path, String data) throws Exception {
        TestClient client = new TestClient();
        client.addData(path, data);
    }

}
