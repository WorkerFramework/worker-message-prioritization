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
package com.github.workerframework.workermessageprioritization.redistribution.config;

import static java.util.stream.Collectors.toList;

import java.util.List;
import java.util.stream.Stream;

import com.google.common.base.MoreObjects;
import com.google.common.base.Strings;
import com.google.inject.Inject;

public final class MessageDistributorConfig {

    private static final String CAF_RABBITMQ_VHOST = "CAF_RABBITMQ_VHOST";
    private static final String CAF_RABBITMQ_VHOST_DEFAULT = "/";

    private static final String CAF_RABBITMQ_URL = "CAF_RABBITMQ_URL";
    private static final String CAF_RABBITMQ_URL_DEFAULT = "amqp://rabbitmq:5672";

    private static final String CAF_RABBITMQ_HOST = "CAF_RABBITMQ_HOST";
    private static final String CAF_RABBITMQ_HOST_DEFAULT = null;

    private static final String CAF_RABBITMQ_PORT = "CAF_RABBITMQ_PORT";
    private static final Integer CAF_RABBITMQ_PORT_DEFAULT = null;

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
    private final String rabbitMQVHost;
    private final String rabbitMQUrl;
    private final String rabbitMQHost;
    private final Integer rabbitMQPort;
    private final String rabbitMQUsername;
    private final String rabbitMQPassword;
    private final String rabbitMQMgmtUrl;
    private final String rabbitMQMgmtUsername;
    private final String rabbitMQMgmtPassword;
    private final int rabbitMQMaxNodeCount;
    private final long distributorRunIntervalMilliseconds;
    private final long consumerPublisherPairLastDoneWorkTimeoutMilliseconds;
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

    @Inject
    public MessageDistributorConfig() {
        rabbitMQVHost = getEnvOrDefault(CAF_RABBITMQ_VHOST, CAF_RABBITMQ_VHOST_DEFAULT);
        rabbitMQUrl = getEnvOrDefault(CAF_RABBITMQ_URL, CAF_RABBITMQ_URL_DEFAULT);
        rabbitMQHost = getEnvOrDefault(CAF_RABBITMQ_HOST, CAF_RABBITMQ_HOST_DEFAULT);
        rabbitMQPort = getEnvOrDefault(CAF_RABBITMQ_PORT, CAF_RABBITMQ_PORT_DEFAULT);
        rabbitMQUsername = getStrEnvOrThrow(CAF_RABBITMQ_USERNAME_ENVVAR);
        rabbitMQPassword = getStrEnvOrThrow(CAF_RABBITMQ_PASSWORD_ENVVAR);
        rabbitMQMgmtUrl = getEnvOrDefault(CAF_RABBITMQ_MGMT_URL, CAF_RABBITMQ_MGMT_URL_DEFAULT);
        rabbitMQMgmtUsername = getStrEnvOrThrow(CAF_RABBITMQ_MGMT_USERNAME_ENVVAR);
        rabbitMQMgmtPassword = getStrEnvOrThrow(CAF_RABBITMQ_MGMT_PASSWORD_ENVVAR);
        rabbitMQMaxNodeCount = getEnvOrDefault(CAF_RABBITMQ_MAX_NODE_COUNT, CAF_RABBITMQ_MAX_NODE_COUNT_DEFAULT);
        distributorRunIntervalMilliseconds = getEnvOrDefault(
            CAF_WMP_DISTRIBUTOR_RUN_INTERVAL_MILLISECONDS,
            CAF_WMP_DISTRIBUTOR_RUN_INTERVAL_MILLISECONDS_DEFAULT);
        consumerPublisherPairLastDoneWorkTimeoutMilliseconds = getEnvOrDefault(
                CAF_WMP_CONSUMER_PUBLISHER_PAIR_LAST_DONE_WORK_TIMEOUT_MILLISECONDS,
                CAF_WMP_CONSUMER_PUBLISHER_PAIR_LAST_DONE_WORK_TIMEOUT_MILLISECONDS_DEFAULT);
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
    }

    public String getRabbitMQVHost() {
        return rabbitMQVHost;
    }

    public String getRabbitMQUrl() {
        return rabbitMQUrl;
    }

    public String getRabbitMQHost() {
        return rabbitMQHost;
    }

    public Integer getRabbitMQPort() {
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

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add(CAF_RABBITMQ_VHOST, rabbitMQVHost)
            .add(CAF_RABBITMQ_URL, rabbitMQUrl)
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
            .add(CAF_WMP_KUBERNETES_NAMESPACES, kubernetesNamespaces)
            .add(CAF_WMP_KUBERNETES_LABEL_CACHE_EXPIRY_MINUTES, kubernetesLabelCacheExpiryMinutes)
            .add(CAF_MIN_TARGET_QUEUE_LENGTH, minTargetQueueLength)
            .add(CAF_MAX_TARGET_QUEUE_LENGTH, maxTargetQueueLength)
            .add(CAF_ROUNDING_MULTIPLE, roundingMultiple)
            .add(CAF_MAX_CONSUMPTION_RATE_HISTORY_SIZE, maxConsumptionRateHistorySize)
            .add(CAF_MIN_CONSUMPTION_RATE_HISTORY_SIZE, minConsumptionRateHistorySize)
            .add(CAF_QUEUE_PROCESSING_TIME_GOAL_SECONDS, queueProcessingTimeGoalSeconds)
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
