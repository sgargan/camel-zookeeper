/**
 *
 */
package org.apache.camel.component.zookeeper;

import org.apache.camel.impl.DefaultMessage;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.data.Stat;

/**
 * <code>ZooKeeperMessage</code> is a {@link org.apache.camel.Message}
 * representing interactions with a ZooKeeper service.
 *
 * @version $
 */
public class ZooKeeperMessage extends DefaultMessage {

    public final static String ZOOKEEPER_NODE = "CamelZooKeeperPath";

    public static final String ZOOKEEPER_NODE_VERSION = "CamelZooKeeperVersion";

    public static final String ZOOKEEPER_ERROR_CODE = "CamelZooKeeperErrorCode";

    public static final String ZOOKEEPER_ACL = "CamelZookeeperAcl";

    public static final String ZOOKEEPER_CREATE_MODE = "CamelZookeeperCreateMode";

    private String path;

    private Stat statistics;

    private Code code;

    public ZooKeeperMessage(String path, Stat statistics) {
        this.path = path;
        this.statistics = statistics;
        this.setHeader(ZOOKEEPER_NODE, path);
    }

    public ZooKeeperMessage(String path, Stat statistics, Code code) {
        this(path, statistics);
        this.code = code;
        this.setHeader(ZOOKEEPER_ERROR_CODE, code);
    }

    public void setCreateMode(CreateMode mode) {
        this.setHeader(ZOOKEEPER_ERROR_CODE, mode.toFlag());
    }

    public CreateMode getCreateMode() {
        return ZooKeeperUtils.getCreateMode(this);
    }

    public Stat getStatistics() {
        return statistics;
    }

    public String getPath() {
        return path;
    }

    public Code getCode() {
        return code;
    }
}
