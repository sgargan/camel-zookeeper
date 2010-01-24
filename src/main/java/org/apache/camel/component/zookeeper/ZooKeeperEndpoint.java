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

import java.util.List;

import org.apache.camel.Consumer;
import org.apache.camel.Processor;
import org.apache.camel.Producer;
import org.apache.camel.impl.DefaultEndpoint;
import org.springframework.jmx.export.annotation.ManagedAttribute;

public class ZooKeeperEndpoint extends DefaultEndpoint {
    private ZooKeeperConfiguration configuration;
    private ZooKeeperConnectionManager connectionManager;

    public ZooKeeperEndpoint(String uri, ZooKeeperComponent component, ZooKeeperConfiguration configuration) {
        super(uri, component);
        this.configuration = configuration;
        this.connectionManager = new ZooKeeperConnectionManager(this);
    }

    public Producer createProducer() throws Exception {
        return new ZookeeperProducer(this);
    }

    public Consumer createConsumer(Processor processor) throws Exception {
        return new ZooKeeperConsumer(this, processor);
    }

    public boolean isSingleton() {
        return true;
    }

    public void setConfiguration(ZooKeeperConfiguration configuration) {
        this.configuration = configuration;
    }

    public ZooKeeperConfiguration getConfiguration() {
        return configuration;
    }

    ZooKeeperConnectionManager getConnectionManager() {
        return connectionManager;
    }

    @ManagedAttribute
    public int getTimeout() {
        return getConfiguration().getTimeout();
    }

    public void setTimeout(int timeout) {
        getConfiguration().setTimeout(timeout);
    }

    @ManagedAttribute
    public boolean getRepeat() {
        return getConfiguration().shouldRepeat();
    }

    public void setRepeat(boolean shouldRepeat) {
        getConfiguration().setRepeat(shouldRepeat);
    }

    @ManagedAttribute
    public List<String> getServers() {
        return getConfiguration().getServers();
    }

    public void setServers(List<String> servers) {
        getConfiguration().setServers(servers);
    }

    @ManagedAttribute
    public boolean shouldReuseConnection() {
        return getConfiguration().shouldReuseConnection();
    }

    public void setReuseConnection(boolean reuseConnection) {
        getConfiguration().setReuseConnection(reuseConnection);
    }

    @ManagedAttribute
    public boolean shouldListChildren() {
        return getConfiguration().listChildren();
    }

    public void setListChildren(boolean listChildren) {
        getConfiguration().setListChildren(listChildren);
    }

    @ManagedAttribute
    public boolean shouldCreate() {
        return getConfiguration().shouldCreate();
    }

    public void setCreate(boolean shouldCreate) {
        getConfiguration().setCreate(shouldCreate);
    }

    @ManagedAttribute
    public long getBackoff() {
        return getConfiguration().getBackoff();
    }

    public void setBackoff(int backoff) {
        getConfiguration().setBackoff(backoff);
    }

    @ManagedAttribute
    public boolean shouldAwaitExistence() {
        return getConfiguration().shouldAwaitExistence();
    }

    public void setAwaitExistence(boolean awaitExistence) {
        getConfiguration().setAwaitExistance(awaitExistence);
    }
}
