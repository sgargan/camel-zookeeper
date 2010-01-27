package org.apache.camel.component.zookeeper;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.camel.Exchange;
import org.apache.camel.InvalidPayloadException;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.mock.MockEndpoint;
import org.apache.camel.component.zookeeper.NaturalSortComparator.Order;
import org.apache.camel.util.ExchangeHelper;
import org.junit.Test;

@SuppressWarnings("unchecked")
public class ConsumeChildrenTest extends ZooKeeperTestSupport {

    @Override
    protected RouteBuilder[] createRouteBuilders() throws Exception {
        return new RouteBuilder[] {new RouteBuilder() {
            public void configure() throws Exception {
                from("zoo://localhost:39913/grimm?repeat=true&listChildren=true")
                    .sortBody(new NaturalSortComparator(Order.Descending)).to("mock:zookeeper-data");
            }
        }};
    }

    @Test
    public void shouldAwaitCreationAndGetDataNotification() throws Exception {

        MockEndpoint mock = getMockEndpoint("mock:zookeeper-data");
        mock.expectedMessageCount(5);

        delay(200);
        client.createPersistent("/grimm", "parent");
        delay(200);
        client.create("/grimm/hansel", "child");
        delay(200);
        client.create("/grimm/gretel", "child");
        delay(200);
        client.delete("/grimm/hansel");
        delay(200);
        client.delete("/grimm/gretel");

        mock.await(5, TimeUnit.SECONDS);
        mock.assertIsSatisfied();

        validateExchangesContainListings(mock, createChildListing(), createChildListing("hansel"), createChildListing("hansel", "gretel"), createChildListing("gretel"),
                                         createChildListing());

    }

    private void validateExchangesContainListings(MockEndpoint mock, List<String>... expected) throws InvalidPayloadException {
        int index = 0;
        for (Exchange received : mock.getReceivedExchanges()) {
            List<String> actual = ExchangeHelper.getMandatoryInBody(received, List.class);
            assertEquals(expected[index++], actual);
        }
    }

    protected void validateChildrenCountChangesEachTime(MockEndpoint mock) {
        int lastChildCount = -1;
        List<Exchange> received = mock.getReceivedExchanges();
        for (int x = 0; x < received.size(); x++) {
            ZooKeeperMessage zkm = (ZooKeeperMessage)mock.getReceivedExchanges().get(x).getIn();
            int childCount = zkm.getStatistics().getNumChildren();
            assertNotSame("Num of children did not change", lastChildCount, childCount);
            lastChildCount = childCount;
        }
    }

}
