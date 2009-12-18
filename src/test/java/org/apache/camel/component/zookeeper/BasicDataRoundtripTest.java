package org.apache.camel.component.zookeeper;

import javax.naming.Context;

import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.dataset.DataSet;
import org.apache.camel.component.dataset.DataSetEndpoint;
import org.apache.camel.component.dataset.SimpleDataSet;
import org.junit.Test;

public class BasicDataRoundtripTest extends ZooKeeperTestSupport {

    private String datasetUri;
    private String zookeeperUri;


    @Override
    protected Context createJndiContext() throws Exception {
        DataSet dataset = new SimpleDataSet(1);

        Context context = super.createJndiContext();
        context.bind("znode-data", dataset);

        return context;
    }

    @Override
    protected RouteBuilder createRouteBuilder() throws Exception {
        return new RouteBuilder() {

            @Override
            public void configure() throws Exception {
                zookeeperUri = "zoo://localhost:39913/roundtrip";
                datasetUri = "dataset:znode-data";
                from(zookeeperUri).to(datasetUri);
                from(datasetUri).to(zookeeperUri);
            }
        };
    }

    @Test
    public void testRoundtripOfDataToAndFromZnode() throws Exception {

        DataSetEndpoint ep = getMandatoryEndpoint(datasetUri, DataSetEndpoint.class);
        ep.await();
        ep.assertIsSatisfied();
    }

}
