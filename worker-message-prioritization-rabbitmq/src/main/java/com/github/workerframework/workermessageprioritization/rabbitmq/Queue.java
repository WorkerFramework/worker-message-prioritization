/*
 * Copyright 2022-2023 Micro Focus or one of its affiliates.
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
import java.util.Map;

public class Queue {
    private String name;
    private long messages;
    private long messages_ready;
    private boolean durable;
    private boolean exclusive;
    private boolean auto_delete;
    private Map<String, Object> arguments;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public long getMessages() {
        return messages;
    }

    public void setMessages(long messages) {
        this.messages = messages;
    }

    public long getMessages_ready() {
        return messages_ready;
    }

    public void setMessages_ready(long messages_ready) {
        this.messages_ready = messages_ready;
    }

    public boolean isDurable() {
        return durable;
    }

    public void setDurable(final boolean durable) {
        this.durable = durable;
    }

    public boolean isExclusive() {
        return exclusive;
    }

    public void setExclusive(final boolean exclusive) {
        this.exclusive = exclusive;
    }

    public boolean isAuto_delete() {
        return auto_delete;
    }

    public void setAuto_delete(final boolean auto_delete) {
        this.auto_delete = auto_delete;
    }

    public Map<String, Object> getArguments() {
        return arguments;
    }

    public void setArguments(final Map<String, Object> arguments) {
        this.arguments = arguments;
    }
    
    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("name", name)
            .add("messages", messages)
            .add("messages_ready", messages_ready)
            .add("durable", durable)
            .add("exclusive", exclusive)
            .add("auto_delete", auto_delete)
            .add("arguments", arguments)
            .toString();
    }
}
