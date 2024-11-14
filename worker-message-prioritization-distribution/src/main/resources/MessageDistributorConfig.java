/*
 * Copyright 2022-2024 Open Text.
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

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.workerframework.workermessageprioritization.rabbitmq.RabbitQueueConstants;
import com.google.common.base.MoreObjects;
import com.google.common.base.Strings;
import com.google.inject.Inject;
import com.github.cafapi.common.util.secret.SecretUtil;

public final class MessageDistributorConfig {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<Map<String, Object>>() {};

    private static final String CAF_RABBITMQ_VHOST = "CAF_RABBITMQ_VHOST";
    private static final String CAF_RABBITMQ_VHOST_DEFAULT = "/";

    private static final String CAF_RABBITMQ_PROTOCOL = "CAF_RABBITMQ_PROTOCOL";
    private static final String CAF_RABBITMQ_PROTOCOL_DEFAULT = "amqp";

    private static final String CAF_RABBITMQ_HOST = "CAF_RABBITMQ_HOST";
    private static final String CAF_RABBITMQ_HOST_DEFAULT = "rabbitmq";

    private static final String CAF_RABBITMQ_PORT = "CAF_RABBITMQ_PORT";
    private static final int CAF_RABBITMQ_PORT_DEFAULT = 5672;

    private static final String CAF_RABBITMQ_USERNAME_ENVVAR = "CAF_RABBITMQ_USERNAME";
    private static final String CAF_RABBITMQ_PASSWORD_ENVVAR = "CAF_RABBITMQ_PASSWORD";

    private static final String CAF_RABBITMQ_MGMT_URL = "CAF_RABBITMQ_MGMT_URL";
    private static final String CAF_RABBITMQ_MGMT_URL_DEFAULT = "http://rabbitmq:15672";

    private static final String CAF_RABBITMQ_MGMT_USERNAME_ENVVAR = "CAF_RABBITMQ_MGMT_USERNAME";
    private static final String CAF_RABBITMQ_MGMT_PASSWORD_ENVVAR = "CAF_RABBITMQ_MGMT_PASSWORD";

    private static final String CAF_RABBITMQ_MAX_NODE_COUNT = "CAF_RABBITMQ_MAX_NODE_COUNT";
    private static final int CAF_RABBITMQ_MAX_NODE_COUNT_DEFAULT = 20;

    private static final String CAF_WMP_DISTRIBUTOR_RUN_INTERVAL_MILLISECONDS = "CAF_WMP_DISTRIBUTOR_RUN_INTERVAL_MILLISECONDS";
    private static final long CAF_WMP_DISTRIBUTOR_RUN_INTERVAL_MILLISECONDS_DEFAULT = 10000;

    private static final String CAF_WMP_CONSUMER_PUBLISHER_PAIR_LAST_DONE_WORK_TIMEOUT_MILLISECONDS
            = "CAF_WMP_CONSUMER_PUBLISHER_PAIR_LAST_DONE_WORK_TIMEOUT_MILLISECONDS";
    private static final long CAF_WMP_CONSUMER_PUBLISHER_PAIR_LAST_DONE_WORK_TIMEOUT_MILLISECONDS_DEFAULT
            = 600000;

    private static final String CAF_WMP_KUBERNETES_ENABLED = "CAF_WMP_KUBERNETES_ENABLED";
    private static final boolean CAF_WMP_KUBERNETES_ENABLED_DEFAULT = true;

    private static final String CAF_WMP_KUBERNETES_NAMESPACES = "CAF_WMP_KUBERNETES_NAMESPACES";

    private static final String CAF_WMP_KUBERNETES_LABEL_CACHE_EXPIRY_MINUTES = "CAF_WMP_KUBERNETES_LABEL_CACHE_EXPIRY_MINUTES";

    private static final int CAF_WMP_KUBERNETES_LABEL_CACHE_EXPIRY_MINUTES_DEFAULT = 60;

    private static final String CAF_ENABLE_TARGET_QUEUE_LENGTH_TUNING = "CAF_ENABLE_TARGET_QUEUE_LENGTH_TUNING";
    private static final boolean CAF_ENABLE_TARGET_QUEUE_LENGTH_TUNING_DEFAULT = false;
    private static final String CAF_MIN_TARGET_QUEUE_LENGTH = "CAF_MIN_TARGET_QUEUE_LENGTH";
    private static final int CAF_MIN_TARGET_QUEUE_LENGTH_DEFAULT = 100;
    private static final String CAF_MAX_TARGET_QUEUE_LENGTH = "CAF_MAX_TARGET_QUEUE_LENGTH";
    private static final int CAF_MAX_TARGET_QUEUE_LENGTH_DEFAULT = 10000000;
    private static final String CAF_ROUNDING_MULTIPLE = "CAF_ROUNDING_MULTIPLE";
    private static final int CAF_ROUNDING_MULTIPLE_DEFAULT = 100;
    private static final String CAF_MAX_CONSUMPTION_RATE_HISTORY_SIZE = "CAF_MAX_CONSUMPTION_RATE_HISTORY_SIZE";
    private static final int CAF_MAX_CONSUMPTION_RATE_HISTORY_SIZE_DEFAULT = 100;
    private static final String CAF_MIN_CONSUMPTION_RATE_HISTORY_SIZE = "CAF_MIN_CONSUMPTION_RATE_HISTORY_SIZE";
    private static final int CAF_MIN_CONSUMPTION_RATE_HISTORY_SIZE_DEFAULT = 10;
    private static final String CAF_QUEUE_PROCESSING_TIME_GOAL_SECONDS = "CAF_QUEUE_PROCESSING_TIME_GOAL_SECONDS";
    private static final int CAF_QUEUE_PROCESSING_TIME_GOAL_SECONDS_DEFAULT = 300;
    private static final String CAF_CONSUMPTION_TARGET_CALCULATOR_MODE = "CAF_CONSUMPTION_TARGET_CALCULATOR_MODE";
    private static final String CAF_CONSUMPTION_TARGET_CALCULATOR_MODE_DEFAULT = "EqualConsumption";

    private static final String CAF_WMP_TARGET_QUEUE_DURABLE = "CAF_WMP_TARGET_QUEUE_DURABLE";
    private static final boolean CAF_WMP_TARGET_QUEUE_DURABLE_DEFAULT = true;
    private static final String CAF_WMP_TARGET_QUEUE_EXCLUSIVE = "CAF_WMP_TARGET_QUEUE_EXCLUSIVE";
    private static final boolean CAF_WMP_TARGET_QUEUE_EXCLUSIVE_DEFAULT = false;
    private static final String CAF_WMP_TARGET_QUEUE_AUTO_DELETE = "CAF_WMP_TARGET_QUEUE_AUTO_DELETE";
    private static final boolean CAF_WMP_TARGET_QUEUE_AUTO_DELETE_DEFAULT = false;
    private static final String CAF_WMP_TARGET_QUEUE_ARGS = "CAF_WMP_TARGET_QUEUE_ARGS";
    private static final Map<String, Object> CAF_WMP_TARGET_QUEUE_ARGS_DEFAULT = Map.of(
            RabbitQueueConstants.RABBIT_PROP_QUEUE_TYPE, RabbitQueueConstants.RABBIT_PROP_QUEUE_TYPE_QUORUM);

    private final String rabbitMQVHost;
    private final String rabbitMQProtocol;
    private final String rabbitMQHost;
    private final int rabbitMQPort;
    private final String rabbitMQUsername;
    private final String rabbitMQPassword;
    private final String rabbitMQMgmtUrl;
    private final String rabbitMQMgmtUsername;
    private final String rabbitMQMgmtPassword;
    private final int rabbitMQMaxNodeCount;
    private final long distributorRunIntervalMilliseconds;
    private final long consumerPublisherPairLastDoneWorkTimeoutMilliseconds;
    private final boolean kubernetesEnabled;
    private final List<String> kubernetesNamespaces;
    private final int kubernetesLabelCacheExpiryMinutes;
    private final boolean enableTargetQueueLengthTuning;
    private final int minTargetQueueLength;
    private final int maxTargetQueueLength;
    private final int roundingMultiple;
    private final int maxConsumptionRateHistorySize;
    private final int minConsumptionRateHistorySize;
    private final int queueProcessingTimeGoalSeconds;
    private final String consumptionTargetCalculatorMode;
    private final boolean targetQueueDurable;
    private final boolean targetQueueExclusive;
    private final boolean targetQueueAutoDelete;
    private final Map<String, Object> targetQueueArgs;

    @Inject
    public MessageDistributorConfig() throws IOException {
        rabbitMQVHost = getEnvOrDefault(CAF_RABBITMQ_VHOST, CAF_RABBITMQ_VHOST_DEFAULT);
        rabbitMQProtocol = getEnvOrDefault(CAF_RABBITMQ_PROTOCOL, CAF_RABBITMQ_PROTOCOL_DEFAULT);
        rabbitMQHost = getEnvOrDefault(CAF_RABBITMQ_HOST, CAF_RABBITMQ_HOST_DEFAULT);
        rabbitMQPort = getEnvOrDefault(CAF_RABBITMQ_PORT, CAF_RABBITMQ_PORT_DEFAULT);
        rabbitMQUsername = getStrEnvOrThrow(CAF_RABBITMQ_USERNAME_ENVVAR);
        rabbitMQPassword = SecretUtil.getSecret(CAF_RABBITMQ_PASSWORD_ENVVAR);
        rabbitMQMgmtUrl = getEnvOrDefault(CAF_RABBITMQ_MGMT_URL, CAF_RABBITMQ_MGMT_URL_DEFAULT);
        rabbitMQMgmtUsername = getStrEnvOrThrow(CAF_RABBITMQ_MGMT_USERNAME_ENVVAR);
        rabbitMQMgmtPassword = SecretUtil.getSecret(CAF_RABBITMQ_MGMT_PASSWORD_ENVVAR);
        rabbitMQMaxNodeCount = getEnvOrDefault(CAF_RABBITMQ_MAX_NODE_COUNT, CAF_RABBITMQ_MAX_NODE_COUNT_DEFAULT);
        distributorRunIntervalMilliseconds = getEnvOrDefault(
            CAF_WMP_DISTRIBUTOR_RUN_INTERVAL_MILLISECONDS,
            CAF_WMP_DISTRIBUTOR_RUN_INTERVAL_MILLISECONDS_DEFAULT);
        consumerPublisherPairLastDoneWorkTimeoutMilliseconds = getEnvOrDefault(
                CAF_WMP_CONSUMER_PUBLISHER_PAIR_LAST_DONE_WORK_TIMEOUT_MILLISECONDS,
                CAF_WMP_CONSUMER_PUBLISHER_PAIR_LAST_DONE_WORK_TIMEOUT_MILLISECONDS_DEFAULT);
        kubernetesEnabled = getEnvOrDefault(CAF_WMP_KUBERNETES_ENABLED, CAF_WMP_KUBERNETES_ENABLED_DEFAULT);
        kubernetesNamespaces = getEnvOrThrow(CAF_WMP_KUBERNETES_NAMESPACES);
        kubernetesLabelCacheExpiryMinutes = getEnvOrDefault(
                CAF_WMP_KUBERNETES_LABEL_CACHE_EXPIRY_MINUTES,
                CAF_WMP_KUBERNETES_LABEL_CACHE_EXPIRY_MINUTES_DEFAULT);
        enableTargetQueueLengthTuning = getEnvOrDefault(CAF_ENABLE_TARGET_QUEUE_LENGTH_TUNING,
                CAF_ENABLE_TARGET_QUEUE_LENGTH_TUNING_DEFAULT);
        minTargetQueueLength = getEnvOrDefault(CAF_MIN_TARGET_QUEUE_LENGTH, CAF_MIN_TARGET_QUEUE_LENGTH_DEFAULT);
        maxTargetQueueLength = getEnvOrDefault(CAF_MAX_TARGET_QUEUE_LENGTH, CAF_MAX_TARGET_QUEUE_LENGTH_DEFAULT);
        roundingMultiple = getEnvOrDefault(CAF_ROUNDING_MULTIPLE, CAF_ROUNDING_MULTIPLE_DEFAULT);
        maxConsumptionRateHistorySize = getEnvOrDefault(CAF_MAX_CONSUMPTION_RATE_HISTORY_SIZE,
                CAF_MAX_CONSUMPTION_RATE_HISTORY_SIZE_DEFAULT);
        minConsumptionRateHistorySize = getEnvOrDefault(CAF_MIN_CONSUMPTION_RATE_HISTORY_SIZE,
                CAF_MIN_CONSUMPTION_RATE_HISTORY_SIZE_DEFAULT);
        queueProcessingTimeGoalSeconds = getEnvOrDefault(CAF_QUEUE_PROCESSING_TIME_GOAL_SECONDS,
                CAF_QUEUE_PROCESSING_TIME_GOAL_SECONDS_DEFAULT);
        consumptionTargetCalculatorMode = getEnvOrDefault(CAF_CONSUMPTION_TARGET_CALCULATOR_MODE,
                CAF_CONSUMPTION_TARGET_CALCULATOR_MODE_DEFAULT);
        targetQueueDurable = getEnvOrDefault(CAF_WMP_TARGET_QUEUE_DURABLE, CAF_WMP_TARGET_QUEUE_DURABLE_DEFAULT);
        targetQueueExclusive = getEnvOrDefault(CAF_WMP_TARGET_QUEUE_EXCLUSIVE, CAF_WMP_TARGET_QUEUE_EXCLUSIVE_DEFAULT);
        targetQueueAutoDelete = getEnvOrDefault(CAF_WMP_TARGET_QUEUE_AUTO_DELETE, CAF_WMP_TARGET_QUEUE_AUTO_DELETE_DEFAULT);
        targetQueueArgs = getEnvOrDefault(CAF_WMP_TARGET_QUEUE_ARGS, CAF_WMP_TARGET_QUEUE_ARGS_DEFAULT);
    }

    public String getRabbitMQVHost() {
        return rabbitMQVHost;
    }

    public String getRabbitmqProtocol() {
        return rabbitMQProtocol;
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
    
    public int getRabbitMQMaxNodeCount() {
        return rabbitMQMaxNodeCount;
    }

    public long getDistributorRunIntervalMilliseconds() {
        return distributorRunIntervalMilliseconds;
    }

    public long getConsumerPublisherPairLastDoneWorkTimeoutMilliseconds() {
        return consumerPublisherPairLastDoneWorkTimeoutMilliseconds;
    }

    public boolean getKubernetesEnabled() {
        return kubernetesEnabled;
    }

    public List<String> getKubernetesNamespaces() {
        return kubernetesNamespaces;
    }

    public int getKubernetesLabelCacheExpiryMinutes() {
        return kubernetesLabelCacheExpiryMinutes;
    }
    public boolean getEnableTargetQueueLengthTuning(){return enableTargetQueueLengthTuning;}
    public int getMinTargetQueueLength(){return minTargetQueueLength;}
    public int getMaxTargetQueueLength(){return maxTargetQueueLength;}
    public int getRoundingMultiple(){return roundingMultiple;}
    public int getMaxConsumptionRateHistorySize(){return maxConsumptionRateHistorySize;}
    public int getMinConsumptionRateHistorySize(){return minConsumptionRateHistorySize;}
    public int getQueueProcessingTimeGoalSeconds(){return queueProcessingTimeGoalSeconds;}
    public String getConsumptionTargetCalculatorMode(){return consumptionTargetCalculatorMode;}

    public boolean isTargetQueueDurable() {
        return targetQueueDurable;
    }

    public boolean isTargetQueueExclusive() {
        return targetQueueExclusive;
    }

    public boolean isTargetQueueAutoDelete() {
        return targetQueueAutoDelete;
    }

    public Map<String,Object> getTargetQueueArgs() {
        return targetQueueArgs;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add(CAF_RABBITMQ_VHOST, rabbitMQVHost)
            .add(CAF_RABBITMQ_PROTOCOL, rabbitMQProtocol)
            .add(CAF_RABBITMQ_HOST, rabbitMQHost)
            .add(CAF_RABBITMQ_PORT, rabbitMQPort)
            .add(CAF_RABBITMQ_USERNAME_ENVVAR, rabbitMQUsername)
            .add(CAF_RABBITMQ_PASSWORD_ENVVAR, "<HIDDEN>")
            .add(CAF_RABBITMQ_MGMT_URL, rabbitMQMgmtUrl)
            .add(CAF_RABBITMQ_MGMT_USERNAME_ENVVAR, rabbitMQMgmtUsername)
            .add(CAF_RABBITMQ_MGMT_PASSWORD_ENVVAR, "<HIDDEN>")
            .add(CAF_RABBITMQ_MAX_NODE_COUNT, rabbitMQMaxNodeCount)
            .add(CAF_WMP_DISTRIBUTOR_RUN_INTERVAL_MILLISECONDS, distributorRunIntervalMilliseconds)
            .add(CAF_WMP_CONSUMER_PUBLISHER_PAIR_LAST_DONE_WORK_TIMEOUT_MILLISECONDS,
                    consumerPublisherPairLastDoneWorkTimeoutMilliseconds)
            .add(CAF_WMP_KUBERNETES_ENABLED, kubernetesEnabled)
            .add(CAF_WMP_KUBERNETES_NAMESPACES, kubernetesNamespaces)
            .add(CAF_WMP_KUBERNETES_LABEL_CACHE_EXPIRY_MINUTES, kubernetesLabelCacheExpiryMinutes)
            .add(CAF_MIN_TARGET_QUEUE_LENGTH, minTargetQueueLength)
            .add(CAF_MAX_TARGET_QUEUE_LENGTH, maxTargetQueueLength)
            .add(CAF_ROUNDING_MULTIPLE, roundingMultiple)
            .add(CAF_MAX_CONSUMPTION_RATE_HISTORY_SIZE, maxConsumptionRateHistorySize)
            .add(CAF_MIN_CONSUMPTION_RATE_HISTORY_SIZE, minConsumptionRateHistorySize)
            .add(CAF_QUEUE_PROCESSING_TIME_GOAL_SECONDS, queueProcessingTimeGoalSeconds)
            .add(CAF_WMP_TARGET_QUEUE_DURABLE, targetQueueDurable)
            .add(CAF_WMP_TARGET_QUEUE_EXCLUSIVE, targetQueueExclusive)
            .add(CAF_WMP_TARGET_QUEUE_AUTO_DELETE, targetQueueAutoDelete)
            .add(CAF_WMP_TARGET_QUEUE_ARGS, targetQueueArgs)
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

    private static boolean getEnvOrDefault(final String name, final boolean defaultValue) {
        final String value = System.getenv(name);

        return !Strings.isNullOrEmpty(value) ? Boolean.parseBoolean(value) : defaultValue;
    }

    private static Map<String, Object> getEnvOrDefault(final String name, final Map<String, Object> defaultValue) {
        final String value = System.getenv(name);

        if (!Strings.isNullOrEmpty(value)) {
            try {
                return OBJECT_MAPPER.readValue(value, MAP_TYPE);
            } catch (final JsonProcessingException e) {
                throw new RuntimeException(String.format("The %s=%s environment variable was not able to be deserialized from JSON",
                        name, value), e);
            }
        } else {
            return defaultValue;
        }
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

    private static String getStrEnvOrThrow(final String name) {
        final String value = System.getenv(name);
        if (Strings.isNullOrEmpty(value)) {
            throw new RuntimeException(String.format("The %s environment variable should not be null or empty.", name));
        }

        return value;
    }
}
