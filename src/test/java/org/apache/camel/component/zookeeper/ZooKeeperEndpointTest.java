package org.apache.camel.component.zookeeper;

import java.util.ArrayList;
import java.util.Set;

import javax.management.Attribute;
import javax.management.ObjectName;

import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.management.JmxInstrumentationUsingDefaultsTest;
import org.springframework.jmx.support.JmxUtils;

@SuppressWarnings("all")
public class ZooKeeperEndpointTest  extends JmxInstrumentationUsingDefaultsTest{

    public synchronized void testEnpointConfigurationCanBeSetViaJMX() throws Exception
    {
        resolveMandatoryEndpoint("zoo:someserver/somepath", ZooKeeperEndpoint.class);
        Set s = mbsc.queryNames(new ObjectName(domainName + ":type=endpoints,*"), null);
        assertEquals("Could not find  endpoints: " + s, 1, s.size());
        ObjectName zepName = new ArrayList<ObjectName>(s).get(0);

        verifyManagedAttribute(zepName, "Path", "/someotherpath");
        verifyManagedAttribute(zepName, "Create", true);
        verifyManagedAttribute(zepName, "Repeat", true);
        verifyManagedAttribute(zepName, "ListChildren", true);
        verifyManagedAttribute(zepName, "AwaitExistence", true);
        verifyManagedAttribute(zepName, "Timeout", 12345);
        verifyManagedAttribute(zepName, "Backoff", 12345l);

        mbsc.invoke(zepName, "clearServers", null, JmxUtils.getMethodSignature(ZooKeeperEndpoint.class.getMethod("clearServers", null)));
        mbsc.invoke(zepName, "addServer", new Object[]{"someserver:12345"}, JmxUtils.getMethodSignature(ZooKeeperEndpoint.class.getMethod("addServer", new Class[]{String.class})));


    }

    private void verifyManagedAttribute(ObjectName zepName, String attributeName, String attributeValue) throws Exception {
        mbsc.setAttribute(zepName, new Attribute(attributeName, attributeValue));
        assertEquals(attributeValue,  mbsc.getAttribute(zepName, attributeName));
    }

    private void verifyManagedAttribute(ObjectName zepName, String attributeName, Integer attributeValue) throws Exception {
        mbsc.setAttribute(zepName, new Attribute(attributeName, attributeValue));
        assertEquals(attributeValue,  mbsc.getAttribute(zepName, attributeName));
    }

    private void verifyManagedAttribute(ObjectName zepName, String attributeName, Boolean attributeValue) throws Exception {
        mbsc.setAttribute(zepName, new Attribute(attributeName, attributeValue));
        assertEquals(attributeValue,  mbsc.getAttribute(zepName, attributeName));
    }

    private void verifyManagedAttribute(ObjectName zepName, String attributeName, Long attributeValue) throws Exception {
        mbsc.setAttribute(zepName, new Attribute(attributeName, attributeValue));
        assertEquals(attributeValue,  mbsc.getAttribute(zepName, attributeName));
    }

    @Override
    public void testCounters() throws Exception {
    }

    @Override
    public void testMBeansRegistered() throws Exception {
    }

    protected RouteBuilder createRouteBuilder() {
        return new RouteBuilder() {
            public void configure() {

            }
        };
    }
}
