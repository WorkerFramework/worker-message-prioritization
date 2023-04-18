/*
 * Copyright 2022-2023 Open Text.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.workerframework.workermessageprioritization.rabbitmq;

import com.google.common.base.MoreObjects;
import java.util.Date;

public class RetrievedShovel extends Shovel {
    private String name;
    private Date timestamp;
    private ShovelState state;
    private String node;
    private String vhost;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Date getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Date timestamp) {
        this.timestamp = timestamp;
    }

    public ShovelState getState() {
        return state;
    }

    public void setState(ShovelState state) {
        this.state = state;
    }

    public String getNode() {
        return node;
    }

    public void setNode(String node) {
        this.node = node;
    }

    public String getVhost() {
        return vhost;
    }

    public void setVhost(String vhost) {
        this.vhost = vhost;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("name", name)
                .add("timestamp", timestamp)
                .add("state", state)
                .add("node", node)
                .add("vhost", vhost)
                .add("ackMode", getAckMode())
                .add("srcUri", getSrcUri())
                .add("srcQueue", getSrcQueue())
                .add("srcDeleteAfter", getSrcDeleteAfter())
                .add("destUri", getDestUri())
                .add("destQueue", getDestQueue())
                .toString();
    }
}
