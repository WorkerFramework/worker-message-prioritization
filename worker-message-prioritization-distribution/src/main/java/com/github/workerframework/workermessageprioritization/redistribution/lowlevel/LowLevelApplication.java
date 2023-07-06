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
package com.github.workerframework.workermessageprioritization.redistribution.lowlevel;

import com.github.workerframework.workermessageprioritization.redistribution.consumption.EqualConsumptionTargetCalculator;
import com.github.workerframework.workermessageprioritization.rabbitmq.QueuesApi;
import com.github.workerframework.workermessageprioritization.rabbitmq.RabbitManagementApi;
import com.github.workerframework.workermessageprioritization.redistribution.config.MessageDistributorConfig;
import com.github.workerframework.workermessageprioritization.targetqueue.TargetQueuePerformanceMetricsProvider;
import com.github.workerframework.workermessageprioritization.targetqueue.HistoricalConsumptionRate;
import com.github.workerframework.workermessageprioritization.targetqueue.RoundTargetQueueLength;
import com.github.workerframework.workermessageprioritization.targetqueue.TunedTargetQueueLengthProvider;
import com.github.workerframework.workermessageprioritization.targetqueue.FixedTargetQueueSettingsProvider;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.concurrent.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LowLevelApplication {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(LowLevelApplication.class);

    public static void main(String[] args) throws IOException, TimeoutException, InterruptedException {

        final ConnectionFactory connectionFactory = new ConnectionFactory();

        final MessageDistributorConfig messageDistributorConfig = new MessageDistributorConfig();

        LOGGER.info("Read the following configuration: {}", messageDistributorConfig);

        connectionFactory.setHost(messageDistributorConfig.getRabbitMQHost());
        connectionFactory.setUsername(messageDistributorConfig.getRabbitMQUsername());
        connectionFactory.setPassword(messageDistributorConfig.getRabbitMQPassword());
        connectionFactory.setPort(messageDistributorConfig.getRabbitMQPort());
        connectionFactory.setVirtualHost("/");

        //https://www.rabbitmq.com/api-guide.html#java-nio
        //connectionFactory.useNio();

        final RabbitManagementApi<QueuesApi> queuesApi = new RabbitManagementApi<>(
            QueuesApi.class,
            messageDistributorConfig.getRabbitMQMgmtUrl(),
            messageDistributorConfig.getRabbitMQMgmtUsername(),
            messageDistributorConfig.getRabbitMQMgmtPassword());

        final TargetQueuePerformanceMetricsProvider targetQueuePerformanceMetricsProvider =
                new TargetQueuePerformanceMetricsProvider(queuesApi);
        final HistoricalConsumptionRate historicalConsumptionRate =
                new HistoricalConsumptionRate(messageDistributorConfig.getMaxConsumptionRateHistorySize(),
                messageDistributorConfig.getMinConsumptionRateHistorySize());
        final RoundTargetQueueLength roundTargetQueueLength = new RoundTargetQueueLength(messageDistributorConfig.getRoundingMultiple());
        final TunedTargetQueueLengthProvider tunedTargetQueueLengthProvider =
                new TunedTargetQueueLengthProvider(
                        targetQueuePerformanceMetricsProvider,
                        historicalConsumptionRate,
                        roundTargetQueueLength,
                        messageDistributorConfig.getNoOpMode(),
                        messageDistributorConfig.getQueueProcessingTimeGoalSeconds());

        final LowLevelDistributor lowLevelDistributor =
                new LowLevelDistributor(
                        queuesApi,
                        connectionFactory,
                        new EqualConsumptionTargetCalculator(new FixedTargetQueueSettingsProvider(), tunedTargetQueueLengthProvider),
                        new StagingTargetPairProvider(),
                        messageDistributorConfig.getDistributorRunIntervalMilliseconds());

        lowLevelDistributor.run();
    }
}
