/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.camel.component.zookeeper;

import static org.apache.camel.component.zookeeper.ZooKeeperMessage.ZOOKEEPER_CREATE_MODE;
import static org.apache.camel.component.zookeeper.ZooKeeperMessage.ZOOKEEPER_NODE;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.camel.Exchange;
import org.apache.camel.ExchangePattern;
import org.apache.camel.Message;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.mock.MockEndpoint;
import org.apache.camel.component.zookeeper.operations.GetChildrenOperation;
import org.apache.camel.util.ExchangeHelper;
import org.apache.zookeeper.CreateMode;
import org.junit.Test;

public class ZookeeperProducerTest extends ZooKeeperTestSupport {

    private String zookeeperUri;

    private String testPayload = "TestPayload";

    @Override
    protected RouteBuilder[] createRouteBuilders() throws Exception {
        return new RouteBuilder[] {new RouteBuilder() {
            public void configure() throws Exception {
                zookeeperUri = "zoo://localhost:39913/node?create=true";
                from("direct:roundtrip").to(zookeeperUri).to("mock:producer-out");
                from(zookeeperUri).to("mock:consumed-from-node");
            }
        }, new RouteBuilder() {
            public void configure() throws Exception {
                from("direct:no-create-fails-set").to("zoo://localhost:39913/doesnotexist");
            }
        }, new RouteBuilder() {
            public void configure() throws Exception {
                from("direct:node-from-header").to("zoo://localhost:39913/notset?create=true");
                from("zoo://localhost:39913/set?create=true").to("mock:consumed-from-set-node");
            }
        }};
    }

    @Test
    public void testRoundtripOfDataToAndFromZnode() throws Exception {
        MockEndpoint mock = getMockEndpoint("mock:consumed-from-node");
        MockEndpoint pipeline = getMockEndpoint("mock:producer-out");
        mock.expectedMessageCount(1);
        pipeline.expectedMessageCount(1);

        Exchange e = createExchangeWithBody(testPayload);
        e.setPattern(ExchangePattern.InOut);
        template.send("direct:roundtrip", e);

        mock.await(2, TimeUnit.SECONDS);
        mock.assertIsSatisfied();
        pipeline.assertIsSatisfied();
    }

    @Test
    public void testAsyncRoundtripOfDataToAndFromZnode() throws Exception {
        MockEndpoint mock = getMockEndpoint("mock:consumed-from-node");
        mock.expectedMessageCount(1);

        Exchange e = createExchangeWithBody(testPayload);
        template.send("direct:roundtrip", e);

        mock.await(2, TimeUnit.SECONDS);
        mock.assertIsSatisfied();
    }

    @Test
    public void setUsingNodeFromHeader() throws Exception {
        MockEndpoint mock = getMockEndpoint("mock:consumed-from-set-node");
        mock.expectedMessageCount(1);

        Exchange e = createExchangeWithBody(testPayload);
        e.setPattern(ExchangePattern.InOut);
        template.sendBodyAndHeader("direct:node-from-header", e, ZOOKEEPER_NODE, "/set");

        mock.await(5, TimeUnit.SECONDS);
        mock.assertIsSatisfied();
    }

    @Test
    public void setUsingCreateModeFromHeader() throws Exception {

        client.createPersistent("/modes-test", "parent for modes");
        for (CreateMode mode : CreateMode.values()) {
            Exchange exchange = createExchangeWithBody(testPayload);
            exchange.getIn().setHeader(ZOOKEEPER_CREATE_MODE, mode);
            exchange.getIn().setHeader(ZOOKEEPER_NODE, "/modes-test/" + mode);
            exchange.setPattern(ExchangePattern.InOut);
            template.send("direct:node-from-header", exchange);
        }
        GetChildrenOperation listing = new GetChildrenOperation(getConnection(), "/modes-test");
        assertEquals(CreateMode.values().length, listing.get().getResult().size());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void setAndGetListing() throws Exception {

        client.createPersistent("/set-listing", "parent for set and list test");

        Exchange exchange = createExchangeWithBody(testPayload);
        exchange.getIn().setHeader(ZOOKEEPER_NODE, "/set-listing/firstborn");
        exchange.setPattern(ExchangePattern.InOut);
        template.send("zoo://localhost:39913/set-listing?create=true&listChildren=true", exchange);
        List<String> children = ExchangeHelper.getMandatoryOutBody(exchange, List.class);
        assertEquals(1, children.size());
        assertEquals("firstborn", children.get(0));
    }

    @Test
    public void testZookeeperMessage() throws Exception {

        MockEndpoint mock = getMockEndpoint("mock:consumed-from-node");
        mock.expectedMessageCount(1);

        Exchange exchange = createExchangeWithBody(testPayload);
        template.send("direct:roundtrip", exchange);
        mock.await();
        mock.assertIsSatisfied();

        Message received = mock.getReceivedExchanges().get(0).getIn();
        assertEquals("/node", ZooKeeperMessage.getPath(received));
        assertNotNull(ZooKeeperMessage.getStatistics(received));
    }
}
