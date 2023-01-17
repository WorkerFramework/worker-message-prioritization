/*
 * Copyright 2022-2022 Micro Focus or one of its affiliates.
 *
 * The only warranties for products and services of Micro Focus and its
 * affiliates and licensors ("Micro Focus") are set forth in the express
 * warranty statements accompanying such products and services. Nothing
 * herein should be construed as constituting an additional warranty.
 * Micro Focus shall not be liable for technical or editorial errors or
 * omissions contained herein. The information contained herein is subject
 * to change without notice.
 *
 * Contains Confidential Information. Except as specifically indicated
 * otherwise, a valid license is required for possession, use or copying.
 * Consistent with FAR 12.211 and 12.212, Commercial Computer Software,
 * Computer Software Documentation, and Technical Data for Commercial
 * Items are licensed to the U.S. Government under vendor's standard
 * commercial license.
 */
package com.github.workerframework.workermessageprioritization.rabbitmq;

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
}
