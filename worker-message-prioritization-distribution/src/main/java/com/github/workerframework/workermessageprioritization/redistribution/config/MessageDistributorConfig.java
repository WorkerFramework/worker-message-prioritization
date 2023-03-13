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

import static java.util.stream.Collectors.toList;

import java.util.List;
import java.util.stream.Stream;

import com.google.common.base.MoreObjects;
import com.google.common.base.Strings;

public final class MessageDistributorConfig {

    private static final String CAF_RABBITMQ_VHOST = "CAF_RABBITMQ_VHOST";
    private static final String CAF_RABBITMQ_VHOST_DEFAULT = "/";

    private static final String CAF_RABBITMQ_HOST = "CAF_RABBITMQ_HOST";
    private static final String CAF_RABBITMQ_HOST_DEFAULT = "rabbitmq";

    private static final String CAF_RABBITMQ_PORT = "CAF_RABBITMQ_PORT";
    private static final int CAF_RABBITMQ_PORT_DEFAULT = 5672;

    private static final String CAF_RABBITMQ_USERNAME = "CAF_RABBITMQ_USERNAME";
    private static final String CAF_RABBITMQ_USERNAME_DEFAULT = "guest";

    private static final String CAF_RABBITMQ_PASSWORD = "CAF_RABBITMQ_PASSWORD";
    private static final String CAF_RABBITMQ_PASSWORD_DEFAULT = "guest";

    private static final String CAF_RABBITMQ_MGMT_URL = "CAF_RABBITMQ_MGMT_URL";
    private static final String CAF_RABBITMQ_MGMT_URL_DEFAULT = "http://rabbitmq:15672";

    private static final String CAF_RABBITMQ_MGMT_USERNAME = "CAF_RABBITMQ_MGMT_USERNAME";
    private static final String CAF_RABBITMQ_MGMT_USERNAME_DEFAULT = "guest";

    private static final String CAF_RABBITMQ_MGMT_PASSWORD = "CAF_RABBITMQ_MGMT_PASSWORD";
    private static final String CAF_RABBITMQ_MGMT_PASSWORD_DEFAULT = "guest";

    private static final String CAF_WMP_DISTRIBUTOR_RUN_INTERVAL_MILLISECONDS = "CAF_WMP_DISTRIBUTOR_RUN_INTERVAL_MILLISECONDS";
    private static final long CAF_WMP_DISTRIBUTOR_RUN_INTERVAL_MILLISECONDS_DEFAULT = 10000;

    private static final String CAF_WMP_NON_RUNNING_SHOVEL_TIMEOUT_MILLISECONDS
        = "CAF_WMP_NON_RUNNING_SHOVEL_TIMEOUT_MILLISECONDS";
    private static final long CAF_WMP_NON_RUNNING_SHOVEL_TIMEOUT_MILLISECONDS_DEFAULT
        = 120000;

    private static final String CAF_WMP_NON_RUNNING_SHOVEL_CHECK_INTERVAL_MILLISECONDS
        = "CAF_WMP_NON_RUNNING_SHOVEL_CHECK_INTERVAL_MILLISECONDS";
    private static final long CAF_WMP_NON_RUNNING_SHOVEL_CHECK_INTERVAL_MILLISECONDS_DEFAULT
        = 120000;

    private static final String CAF_WMP_SHOVEL_RUNNING_TOO_LONG_TIMEOUT_MILLISECONDS
            = "CAF_WMP_SHOVEL_RUNNING_TOO_LONG_TIMEOUT_MILLISECONDS";
    private static final long CAF_WMP_SHOVEL_RUNNING_TOO_LONG_TIMEOUT_MILLISECONDS_DEFAULT
            = 1800000;

    private static final String CAF_WMP_SHOVEL_RUNNING_TOO_LONG_CHECK_INTERVAL_MILLISECONDS
            = "CAF_WMP_SHOVEL_RUNNING_TOO_LONG_CHECK_INTERVAL_MILLISECONDS";
    private static final long CAF_WMP_SHOVEL_RUNNING_TOO_LONG_CHECK_INTERVAL_MILLISECONDS_DEFAULT
            = 120000;

    private static final String CAF_WMP_KUBERNETES_NAMESPACES = "CAF_WMP_KUBERNETES_NAMESPACES";

    private static final String CAF_WMP_KUBERNETES_LABEL_CACHE_EXPIRY_MINUTES = "CAF_WMP_KUBERNETES_LABEL_CACHE_EXPIRY_MINUTES";

    private static final int CAF_WMP_KUBERNETES_LABEL_CACHE_EXPIRY_MINUTES_DEFAULT = 60;

    private final String rabbitMQVHost;
    private final String rabbitMQHost;
    private final int rabbitMQPort;
    private final String rabbitMQUsername;
    private final String rabbitMQPassword;
    private final String rabbitMQMgmtUrl;
    private final String rabbitMQMgmtUsername;
    private final String rabbitMQMgmtPassword;
    private final long distributorRunIntervalMilliseconds;
    private final long nonRunningShovelTimeoutMilliseconds;
    private final long nonRunningShovelCheckIntervalMilliseconds;
    private final long shovelRunningTooLongTimeoutMilliseconds;
    private final long shovelRunningTooLongCheckIntervalMilliseconds;

    private final List<String> kubernetesNamespaces;
    private final int kubernetesLabelCacheExpiryMinutes;

    public MessageDistributorConfig() {
        rabbitMQVHost = getEnvOrDefault(CAF_RABBITMQ_VHOST, CAF_RABBITMQ_VHOST_DEFAULT);
        rabbitMQHost = getEnvOrDefault(CAF_RABBITMQ_HOST, CAF_RABBITMQ_HOST_DEFAULT);
        rabbitMQPort = getEnvOrDefault(CAF_RABBITMQ_PORT, CAF_RABBITMQ_PORT_DEFAULT);
        rabbitMQUsername = getEnvOrDefault(CAF_RABBITMQ_USERNAME, CAF_RABBITMQ_USERNAME_DEFAULT);
        rabbitMQPassword = getEnvOrDefault(CAF_RABBITMQ_PASSWORD, CAF_RABBITMQ_PASSWORD_DEFAULT);
        rabbitMQMgmtUrl = getEnvOrDefault(CAF_RABBITMQ_MGMT_URL, CAF_RABBITMQ_MGMT_URL_DEFAULT);
        rabbitMQMgmtUsername = getEnvOrDefault(CAF_RABBITMQ_MGMT_USERNAME, CAF_RABBITMQ_MGMT_USERNAME_DEFAULT);
        rabbitMQMgmtPassword = getEnvOrDefault(CAF_RABBITMQ_MGMT_PASSWORD, CAF_RABBITMQ_MGMT_PASSWORD_DEFAULT);
        distributorRunIntervalMilliseconds = getEnvOrDefault(
            CAF_WMP_DISTRIBUTOR_RUN_INTERVAL_MILLISECONDS,
            CAF_WMP_DISTRIBUTOR_RUN_INTERVAL_MILLISECONDS_DEFAULT);
        nonRunningShovelTimeoutMilliseconds = getEnvOrDefault(
            CAF_WMP_NON_RUNNING_SHOVEL_TIMEOUT_MILLISECONDS,
            CAF_WMP_NON_RUNNING_SHOVEL_TIMEOUT_MILLISECONDS_DEFAULT);
        nonRunningShovelCheckIntervalMilliseconds = getEnvOrDefault(
            CAF_WMP_NON_RUNNING_SHOVEL_CHECK_INTERVAL_MILLISECONDS,
            CAF_WMP_NON_RUNNING_SHOVEL_CHECK_INTERVAL_MILLISECONDS_DEFAULT);
        shovelRunningTooLongTimeoutMilliseconds = getEnvOrDefault(
                CAF_WMP_SHOVEL_RUNNING_TOO_LONG_TIMEOUT_MILLISECONDS,
                CAF_WMP_SHOVEL_RUNNING_TOO_LONG_TIMEOUT_MILLISECONDS_DEFAULT);
        shovelRunningTooLongCheckIntervalMilliseconds = getEnvOrDefault(
                CAF_WMP_SHOVEL_RUNNING_TOO_LONG_CHECK_INTERVAL_MILLISECONDS,
                CAF_WMP_SHOVEL_RUNNING_TOO_LONG_CHECK_INTERVAL_MILLISECONDS_DEFAULT);
        kubernetesNamespaces = getEnvOrThrow(CAF_WMP_KUBERNETES_NAMESPACES);
        kubernetesLabelCacheExpiryMinutes = getEnvOrDefault(
                CAF_WMP_KUBERNETES_LABEL_CACHE_EXPIRY_MINUTES,
                CAF_WMP_KUBERNETES_LABEL_CACHE_EXPIRY_MINUTES_DEFAULT);
    }

    public String getRabbitMQVHost() {
        return rabbitMQVHost;
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

    public long getNonRunningShovelTimeoutMilliseconds() {
        return nonRunningShovelTimeoutMilliseconds;
    }

    public long getNonRunningShovelCheckIntervalMilliseconds() {
        return nonRunningShovelCheckIntervalMilliseconds;
    }

    public long getShovelRunningTooLongTimeoutMilliseconds() {
        return shovelRunningTooLongTimeoutMilliseconds;
    }

    public long getShovelRunningTooLongCheckIntervalMilliseconds() {
        return shovelRunningTooLongCheckIntervalMilliseconds;
    }

    public long getDistributorRunIntervalMilliseconds() {
        return distributorRunIntervalMilliseconds;
    }

    public List<String> getKubernetesNamespaces() {
        return kubernetesNamespaces;
    }

    public int getKubernetesLabelCacheExpiryMinutes() {
        return kubernetesLabelCacheExpiryMinutes;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add(CAF_RABBITMQ_VHOST, rabbitMQVHost)
            .add(CAF_RABBITMQ_HOST, rabbitMQHost)
            .add(CAF_RABBITMQ_PORT, rabbitMQPort)
            .add(CAF_RABBITMQ_USERNAME, rabbitMQUsername)
            .add(CAF_RABBITMQ_PASSWORD, "<HIDDEN>")
            .add(CAF_RABBITMQ_MGMT_URL, rabbitMQMgmtUrl)
            .add(CAF_RABBITMQ_MGMT_USERNAME, rabbitMQMgmtUsername)
            .add(CAF_RABBITMQ_MGMT_PASSWORD, "<HIDDEN>")
            .add(CAF_WMP_DISTRIBUTOR_RUN_INTERVAL_MILLISECONDS, distributorRunIntervalMilliseconds)
            .add(CAF_WMP_NON_RUNNING_SHOVEL_TIMEOUT_MILLISECONDS, nonRunningShovelTimeoutMilliseconds)
            .add(CAF_WMP_NON_RUNNING_SHOVEL_CHECK_INTERVAL_MILLISECONDS, nonRunningShovelCheckIntervalMilliseconds)
            .add(CAF_WMP_SHOVEL_RUNNING_TOO_LONG_TIMEOUT_MILLISECONDS, shovelRunningTooLongTimeoutMilliseconds)
            .add(CAF_WMP_SHOVEL_RUNNING_TOO_LONG_CHECK_INTERVAL_MILLISECONDS, shovelRunningTooLongCheckIntervalMilliseconds)
            .add(CAF_WMP_KUBERNETES_NAMESPACES, kubernetesNamespaces)
            .add(CAF_WMP_KUBERNETES_LABEL_CACHE_EXPIRY_MINUTES, kubernetesLabelCacheExpiryMinutes)
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
    
    private static long getEnvOrDefault(final String name, final long defaultValue) {
        final String value = System.getenv(name);

        return !Strings.isNullOrEmpty(value) ? Long.parseLong(value) : defaultValue;
    }

    private static List<String> getEnvOrThrow(final String name) {
        final List<String> values = Stream.of(Strings.nullToEmpty(System.getenv(name)).split(","))
                        .map(String::trim)
                        .filter(s -> !Strings.isNullOrEmpty(s))
                        .collect(toList());

        if (values.isEmpty()) {
            throw new RuntimeException(String.format("The %s environment variable should not be null or empty. " +
                    "If multiple values are provided, they should be comma separated.", name));
        }

        return values;
    }
}
