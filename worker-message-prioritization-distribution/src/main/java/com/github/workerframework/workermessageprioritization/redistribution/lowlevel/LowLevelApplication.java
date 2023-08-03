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

import com.github.workerframework.workermessageprioritization.redistribution.DistributorModule;
import com.github.workerframework.workermessageprioritization.redistribution.consumption.ConsumptionTargetCalculator;
import com.github.workerframework.workermessageprioritization.redistribution.consumption.EqualConsumptionTargetCalculator;
import com.github.workerframework.workermessageprioritization.rabbitmq.QueuesApi;
import com.github.workerframework.workermessageprioritization.rabbitmq.RabbitManagementApi;
import com.github.workerframework.workermessageprioritization.redistribution.config.MessageDistributorConfig;
import com.github.workerframework.workermessageprioritization.targetqueue.QueueConsumptionRateProvider;
import com.github.workerframework.workermessageprioritization.targetqueue.HistoricalConsumptionRateManager;
import com.github.workerframework.workermessageprioritization.targetqueue.TargetQueueLengthRounder;
import com.github.workerframework.workermessageprioritization.targetqueue.TargetQueueSettingsProvider;
import com.github.workerframework.workermessageprioritization.targetqueue.TunedTargetQueueLengthProvider;
import com.github.workerframework.workermessageprioritization.targetqueue.K8sTargetQueueSettingsProvider;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.concurrent.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LowLevelApplication {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(LowLevelApplication.class);

    public static void main(String[] args) throws IOException, TimeoutException, InterruptedException {

        final Injector injector = Guice.createInjector(new DistributorModule());
        //TODO Ultimately the following two lines will replace everything below
//        final LowLevelDistributor lowLevelDistributor = injector.getInstance(LowLevelDistributor.class);
//        lowLevelDistributor.run();

        final MessageDistributorConfig messageDistributorConfig = injector.getInstance(MessageDistributorConfig.class);

        final ConnectionFactory connectionFactory = new ConnectionFactory();

        LOGGER.info("Read the following configuration: {}", messageDistributorConfig);

        connectionFactory.setHost(messageDistributorConfig.getRabbitMQHost());
        connectionFactory.setUsername(messageDistributorConfig.getRabbitMQUsername());
        connectionFactory.setPassword(messageDistributorConfig.getRabbitMQPassword());
        connectionFactory.setPort(messageDistributorConfig.getRabbitMQPort());
        connectionFactory.setVirtualHost(messageDistributorConfig.getRabbitMQVHost());

        //https://www.rabbitmq.com/api-guide.html#java-nio
        //connectionFactory.useNio();

        final RabbitManagementApi<QueuesApi> queuesApi = new RabbitManagementApi<>(
            QueuesApi.class,
            messageDistributorConfig.getRabbitMQMgmtUrl(),
            messageDistributorConfig.getRabbitMQMgmtUsername(),
            messageDistributorConfig.getRabbitMQMgmtPassword());
        
        //TODO In the meantime we'll comment out each manual construction and replace it with a call to injector.getInstance
//        final TargetQueueSettingsProvider k8sTargetQueueSettingsProvider = new K8sTargetQueueSettingsProvider(
//                messageDistributorConfig.getKubernetesNamespaces(),
//                messageDistributorConfig.getKubernetesLabelCacheExpiryMinutes());

        //TODO When we have all of the dependencies working then we can delete all this code
        final TargetQueueSettingsProvider targetQueueSettingsProvider = injector.getInstance(TargetQueueSettingsProvider.class);

        final QueueConsumptionRateProvider queueConsumptionRateProvider =
                new QueueConsumptionRateProvider(queuesApi);
        final HistoricalConsumptionRateManager historicalConsumptionRateManager =
                new HistoricalConsumptionRateManager(messageDistributorConfig.getMaxConsumptionRateHistorySize(),
                messageDistributorConfig.getMinConsumptionRateHistorySize());
        final TargetQueueLengthRounder targetQueueLengthRounder = new TargetQueueLengthRounder(messageDistributorConfig.getRoundingMultiple());
        final TunedTargetQueueLengthProvider tunedTargetQueueLengthProvider =
                new TunedTargetQueueLengthProvider(
                        queueConsumptionRateProvider,
                        historicalConsumptionRateManager,
                        targetQueueLengthRounder,
                        messageDistributorConfig.getNoOpMode(),
                        messageDistributorConfig.getQueueProcessingTimeGoalSeconds());

        final ConsumptionTargetCalculator consumptionTargetCalculator =
                new EqualConsumptionTargetCalculator(targetQueueSettingsProvider, tunedTargetQueueLengthProvider);

        final LowLevelDistributor lowLevelDistributor =
                new LowLevelDistributor(
                        queuesApi,
                        connectionFactory,
                        consumptionTargetCalculator,
                        new StagingTargetPairProvider(),
                        messageDistributorConfig.getDistributorRunIntervalMilliseconds(),
                        messageDistributorConfig.getConsumerPublisherPairLastDoneWorkTimeoutMilliseconds(),
                        messageDistributorConfig.getMaxTargetQueueLength(),
                        messageDistributorConfig.getMinTargetQueueLength());

        lowLevelDistributor.run();
    }
}
