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

import java.util.ArrayList;
import java.util.List;

import org.apache.camel.RuntimeCamelException;

/**
 * <code>ZookeeperConfiguration</code> encapsulates the configuration that may
 * be applied to a {@link ZookeeperComponent} and inherited by the
 * {@link ZookeeperEndpoint}s it creates.
 * 
 * @version $
 */
public class ZookeeperConfiguration implements Cloneable {

    private int timeout;
    private List<String> servers;
    private boolean reuseConnection = true;

    public void addZookeeperServer(String server) {
        if (servers == null) {
            servers = new ArrayList<String>();
        }
        servers.add(server);
    }

    public List<String> getServers() {
        return servers;
    }

    public void setServers(List<String> servers) {
        this.servers = servers;
    }

    public boolean shouldReuseConnection() {
        return reuseConnection;
    }

    public void setReuseConnection(boolean reuseConnection) {
        this.reuseConnection = reuseConnection;
    }

    public int getTimeout() {
        return timeout;
    }

    public void setTimeout(int timeout) {
        this.timeout = timeout;
    }

    public ZookeeperConfiguration copy() {
        try {
            return (ZookeeperConfiguration)clone();
        } catch (CloneNotSupportedException e) {
            throw new RuntimeCamelException(e);
        }
    }

}
