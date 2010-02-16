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

import org.apache.camel.impl.DefaultMessage;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.data.Stat;

/**
 * <code>ZooKeeperMessage</code> is a {@link org.apache.camel.Message}
 * representing interactions with a ZooKeeper service. It contains a number of
 * optional Header Constants that are used by the Producer and consumer
 * mechanisms to finely control these interactions.
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
