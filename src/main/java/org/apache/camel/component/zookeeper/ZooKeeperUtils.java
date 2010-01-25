package org.apache.camel.component.zookeeper;

import java.util.List;

import org.apache.camel.Exchange;
import org.apache.camel.Message;
import org.apache.camel.util.ExchangeHelper;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.ACL;

public class ZooKeeperUtils {

    /**
     * Pulls a createMode flag from the header keyed by @see
     * {@link ZooKeeperMessage.ZOOKEEPER_CREATE_MODE} in the given message and attemps to
     * pares a {@link CreateMode} from it.
     *
     * @param message the message that may contain a ZOOKEEPER_CREATE_MODE header.
     *
     * @return the parsed {@link CreateMode} or null if the header was null or not a valid mode flag.
     */
    public static CreateMode getCreateMode(Message message) {
        CreateMode mode = null;
        Integer modeHeader = message.getHeader(ZooKeeperMessage.ZOOKEEPER_CREATE_MODE, Integer.class);
        if (mode != null) {
            try {
                mode = CreateMode.fromFlag(modeHeader);
            } catch (Exception e) {
            }
        }
        return mode;
    }

    public static <T> T getZookeeperProperty(Message m, String propertyName, T defaultValue, Class<? extends T> type) {
        T value = m.getHeader(propertyName, type);
        if (value == null) {
            value = defaultValue;
        }
        return value;
    }

    public static byte[] getPayloadFromExchange(Exchange exchange) {
        return ExchangeHelper.convertToType(exchange, byte[].class, exchange.getIn().getBody());
    }

    @SuppressWarnings("unchecked")
    public static List<ACL> getAclList(Message in) {
        return (List<ACL> )getZookeeperProperty(in, ZooKeeperMessage.ZOOKEEPER_ACL, Ids.ANYONE_ID_UNSAFE, List.class);
    }
}
