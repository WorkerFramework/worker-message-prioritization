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

    private static final String CAF_RABBITMQ_HOST
        = "CAF_RABBITMQ_HOST";
    private static final String CAF_RABBITMQ_HOST_DEFAULT = "rabbitmq";

    private static final String CAF_RABBITMQ_PORT
        = "CAF_RABBITMQ_PORT";
    private static final int CAF_RABBITMQ_PORT_DEFAULT = 5672;

    private static final String CAF_RABBITMQ_USERNAME
        = "CAF_RABBITMQ_USERNAME";
    private static final String CAF_RABBITMQ_USERNAME_DEFAULT = "guest";

    private static final String CAF_RABBITMQ_PASSWORD
        = "CAF_RABBITMQ_PASSWORD";
    private static final String CAF_RABBITMQ_PASSWORD_DEFAULT = "guest";

    private static final String CAF_RABBITMQ_MGMT_URL
        = "CAF_RABBITMQ_MGMT_URL";
    private static final String CAF_RABBITMQ_MGMT_URL_DEFAULT = "http://rabbitmq:15672";

    private static final String CAF_RABBITMQ_MGMT_USERNAME
        = "CAF_RABBITMQ_MGMT_USERNAME";
    private static final String CAF_RABBITMQ_MGMT_USERNAME_DEFAULT = "guest";

    private static final String CAF_RABBITMQ_MGMT_PASSWORD
        = "CAF_RABBITMQ_MGMT_PASSWORD";
    private static final String CAF_RABBITMQ_MGMT_PASSWORD_DEFAULT = "guest";

    private final String rabbitMQHost;
    private final int rabbitMQPort;
    private final String rabbitMQUsername;
    private final String rabbitMQPassword;
    private final String rabbitMQMgmtUrl;
    private final String rabbitMQMgmtUsername;
    private final String rabbitMQMgmtPassword;

    public MessageDistributorConfig() {
        rabbitMQHost = getEnvOrDefault(
            CAF_RABBITMQ_HOST, CAF_RABBITMQ_HOST_DEFAULT);
        rabbitMQPort = getEnvOrDefault(
            CAF_RABBITMQ_PORT, CAF_RABBITMQ_PORT_DEFAULT);
        rabbitMQUsername = getEnvOrDefault(
            CAF_RABBITMQ_USERNAME, CAF_RABBITMQ_USERNAME_DEFAULT);
        rabbitMQPassword = getEnvOrDefault(
            CAF_RABBITMQ_PASSWORD, CAF_RABBITMQ_PASSWORD_DEFAULT);

        rabbitMQMgmtUrl = getEnvOrDefault(
            CAF_RABBITMQ_MGMT_URL, CAF_RABBITMQ_MGMT_URL_DEFAULT);
        rabbitMQMgmtUsername = getEnvOrDefault(
            CAF_RABBITMQ_MGMT_USERNAME, CAF_RABBITMQ_MGMT_USERNAME_DEFAULT);
        rabbitMQMgmtPassword = getEnvOrDefault(
            CAF_RABBITMQ_MGMT_PASSWORD, CAF_RABBITMQ_MGMT_PASSWORD_DEFAULT);
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

    public String getRabbitMQMgmtUrl() {
        return rabbitMQMgmtUrl;
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
            .add(CAF_RABBITMQ_HOST, rabbitMQHost)
            .add(CAF_RABBITMQ_PORT, rabbitMQPort)
            .add(CAF_RABBITMQ_USERNAME, rabbitMQUsername)
            .add(CAF_RABBITMQ_PASSWORD, "<HIDDEN>")
            .add(CAF_RABBITMQ_MGMT_URL, rabbitMQMgmtUrl)
            .add(CAF_RABBITMQ_MGMT_USERNAME, rabbitMQMgmtUsername)
            .add(CAF_RABBITMQ_MGMT_PASSWORD, "<HIDDEN>")
            .toString();
    }

    private static String getEnvOrDefault(final String name, final String defaultValue) {
        final String value = System.getenv(name);

        return !Strings.isNullOrEmpty(value) ? value : defaultValue;
    }

    private static int getEnvOrDefault(final String name, final int defaultValue) {
        final String value = System.getenv(name);

        return !Strings.isNullOrEmpty(value) ? Integer.parseInt(value) : defaultValue;
    }
}
