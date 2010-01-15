/**
 *
 */
package org.apache.camel.component.zookeeper;

import org.apache.camel.impl.DefaultMessage;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.data.Stat;

/**
 * <code>ZooKeeperMessage</code> is a {@link org.apache.camel.Message}
 * representing interactions with a ZooKeeper service.
 *
 * @version $
 */
public class ZooKeeperMessage extends DefaultMessage {

    public final static String ZOOKEEPER_PATH = "CamelZooKeeperPath";

    public final static String ZOOKEEPER_CHILDREN = "CamelZooKeeperChildren";

    public static final String ZOOKEEPER_ZNODE_VERSION = "CamelZooKeeperVersion";

    public static final String ZOOKEEPER_ERROR_CODE = "CamelZooKeeperErrorCode";

    private String path;

    private Stat statistics;

    private Code code;

    public ZooKeeperMessage(String path, Stat statistics) {
        this.path = path;
        this.statistics = statistics;
        this.setHeader(ZOOKEEPER_PATH, path);

    }

    public ZooKeeperMessage(String path, Stat statistics, Code code) {
        this(path, statistics);
        this.code = code;
        this.setHeader(ZOOKEEPER_ERROR_CODE, code);
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
