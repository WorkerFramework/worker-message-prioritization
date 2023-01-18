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
package com.github.workerframework.workermessageprioritization.redistribution.config;

import com.google.common.base.MoreObjects;
import com.google.common.base.Strings;

public final class MessageDistributorConfig {

    private static final String WORKER_MESSAGE_PRIORITIZATION_RABBITMQ_HOST
        = "WORKER_MESSAGE_PRIORITIZATION_RABBITMQ_HOST";
    private static final String WORKER_MESSAGE_PRIORITIZATION_RABBITMQ_HOST_DEFAULT = "rabbitmq";

    private static final String WORKER_MESSAGE_PRIORITIZATION_RABBITMQ_PORT
        = "WORKER_MESSAGE_PRIORITIZATION_RABBITMQ_PORT";
    private static final int WORKER_MESSAGE_PRIORITIZATION_RABBITMQ_PORT_DEFAULT = 5672;

    private static final String WORKER_MESSAGE_PRIORITIZATION_RABBITMQ_USERNAME
        = "WORKER_MESSAGE_PRIORITIZATION_RABBITMQ_USERNAME";
    private static final String WORKER_MESSAGE_PRIORITIZATION_RABBITMQ_USERNAME_DEFAULT = "guest";

    private static final String WORKER_MESSAGE_PRIORITIZATION_RABBITMQ_PASSWORD
        = "WORKER_MESSAGE_PRIORITIZATION_RABBITMQ_PASSWORD";
    private static final String WORKER_MESSAGE_PRIORITIZATION_RABBITMQ_PASSWORD_DEFAULT = "guest";

    private static final String WORKER_MESSAGE_PRIORITIZATION_RABBITMQ_MGMT_PROTOCOL
        = "WORKER_MESSAGE_PRIORITIZATION_RABBITMQ_MGMT_PROTOCOL";
    private static final String WORKER_MESSAGE_PRIORITIZATION_RABBITMQ_MGMT_PROTOCOL_DEFAULT = "http";

    private static final String WORKER_MESSAGE_PRIORITIZATION_RABBITMQ_MGMT_HOST
        = "WORKER_MESSAGE_PRIORITIZATION_RABBITMQ_MGMT_HOST";
    private static final String WORKER_MESSAGE_PRIORITIZATION_RABBITMQ_MGMT_HOST_DEFAULT = "rabbitmq";

    private static final String WORKER_MESSAGE_PRIORITIZATION_RABBITMQ_MGMT_PORT
        = "WORKER_MESSAGE_PRIORITIZATION_RABBITMQ_MGMT_PORT";
    private static final int WORKER_MESSAGE_PRIORITIZATION_RABBITMQ_MGMT_PORT_DEFAULT = 15672;

    private static final String WORKER_MESSAGE_PRIORITIZATION_RABBITMQ_MGMT_USERNAME
        = "WORKER_MESSAGE_PRIORITIZATION_RABBITMQ_MGMT_USERNAME";
    private static final String WORKER_MESSAGE_PRIORITIZATION_RABBITMQ_MGMT_USERNAME_DEFAULT = "guest";

    private static final String WORKER_MESSAGE_PRIORITIZATION_RABBITMQ_MGMT_PASSWORD
        = "WORKER_MESSAGE_PRIORITIZATION_RABBITMQ_MGMT_PASSWORD";
    private static final String WORKER_MESSAGE_PRIORITIZATION_RABBITMQ_MGMT_PASSWORD_DEFAULT = "guest";

    private final String rabbitMQHost;
    private final int rabbitMQPort;
    private final String rabbitMQUsername;
    private final String rabbitMQPassword;
    private final String rabbitMQMgmtProtocol;
    private final String rabbitMQMgmtHost;
    private final int rabbitMQMgmtPort;
    private final String rabbitMQMgmtUsername;
    private final String rabbitMQMgmtPassword;

    public MessageDistributorConfig() {
        rabbitMQHost = getEnvOrDefault(
            WORKER_MESSAGE_PRIORITIZATION_RABBITMQ_HOST, WORKER_MESSAGE_PRIORITIZATION_RABBITMQ_HOST_DEFAULT);
        rabbitMQPort = getEnvOrDefault(
            WORKER_MESSAGE_PRIORITIZATION_RABBITMQ_PORT, WORKER_MESSAGE_PRIORITIZATION_RABBITMQ_PORT_DEFAULT);
        rabbitMQUsername = getEnvOrDefault(
            WORKER_MESSAGE_PRIORITIZATION_RABBITMQ_USERNAME, WORKER_MESSAGE_PRIORITIZATION_RABBITMQ_USERNAME_DEFAULT);
        rabbitMQPassword = getEnvOrDefault(
            WORKER_MESSAGE_PRIORITIZATION_RABBITMQ_PASSWORD, WORKER_MESSAGE_PRIORITIZATION_RABBITMQ_PASSWORD_DEFAULT);

        rabbitMQMgmtProtocol = getEnvOrDefault(
            WORKER_MESSAGE_PRIORITIZATION_RABBITMQ_MGMT_PROTOCOL, WORKER_MESSAGE_PRIORITIZATION_RABBITMQ_MGMT_PROTOCOL_DEFAULT);
        rabbitMQMgmtHost = getEnvOrDefault(
            WORKER_MESSAGE_PRIORITIZATION_RABBITMQ_MGMT_HOST, WORKER_MESSAGE_PRIORITIZATION_RABBITMQ_MGMT_HOST_DEFAULT);
        rabbitMQMgmtPort = getEnvOrDefault(
            WORKER_MESSAGE_PRIORITIZATION_RABBITMQ_MGMT_PORT, WORKER_MESSAGE_PRIORITIZATION_RABBITMQ_MGMT_PORT_DEFAULT);
        rabbitMQMgmtUsername = getEnvOrDefault(
            WORKER_MESSAGE_PRIORITIZATION_RABBITMQ_MGMT_USERNAME, WORKER_MESSAGE_PRIORITIZATION_RABBITMQ_MGMT_USERNAME_DEFAULT);
        rabbitMQMgmtPassword = getEnvOrDefault(
            WORKER_MESSAGE_PRIORITIZATION_RABBITMQ_MGMT_PASSWORD, WORKER_MESSAGE_PRIORITIZATION_RABBITMQ_MGMT_PASSWORD_DEFAULT);
    }

    public String getRabbitMQHost() {
        return rabbitMQHost;
    }

    public int getRabbitMQPort() {
        return rabbitMQPort;
    }

    public String getRabbitMQUsername() {
        return rabbitMQUsername;
    }

    public String getRabbitMQPassword() {
        return rabbitMQPassword;
    }

    public String getRabbitMQMgmtProtocol() {
        return rabbitMQMgmtProtocol;
    }

    public String getRabbitMQMgmtHost() {
        return rabbitMQMgmtHost;
    }

    public int getRabbitMQMgmtPort() {
        return rabbitMQMgmtPort;
    }

    public String getRabbitMQMgmtUsername() {
        return rabbitMQMgmtUsername;
    }

    public String getRabbitMQMgmtPassword() {
        return rabbitMQMgmtPassword;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add(WORKER_MESSAGE_PRIORITIZATION_RABBITMQ_HOST, rabbitMQHost)
            .add(WORKER_MESSAGE_PRIORITIZATION_RABBITMQ_PORT, rabbitMQPort)
            .add(WORKER_MESSAGE_PRIORITIZATION_RABBITMQ_USERNAME, rabbitMQUsername)
            .add(WORKER_MESSAGE_PRIORITIZATION_RABBITMQ_PASSWORD, "<HIDDEN>")
            .add(WORKER_MESSAGE_PRIORITIZATION_RABBITMQ_MGMT_PROTOCOL, rabbitMQMgmtProtocol)
            .add(WORKER_MESSAGE_PRIORITIZATION_RABBITMQ_MGMT_HOST, rabbitMQMgmtHost)
            .add(WORKER_MESSAGE_PRIORITIZATION_RABBITMQ_MGMT_PORT, rabbitMQMgmtPort)
            .add(WORKER_MESSAGE_PRIORITIZATION_RABBITMQ_MGMT_USERNAME, rabbitMQMgmtUsername)
            .add(WORKER_MESSAGE_PRIORITIZATION_RABBITMQ_MGMT_PASSWORD, "<HIDDEN>")
            .toString();
    }

    private String getEnvOrDefault(final String name, final String defaultValue) {
        final String value = System.getenv(name);

        return !Strings.isNullOrEmpty(value) ? value : defaultValue;
    }

    private int getEnvOrDefault(final String name, final int defaultValue) {
        final String value = System.getenv(name);

        return !Strings.isNullOrEmpty(value) ? Integer.parseInt(value) : defaultValue;
    }
}
