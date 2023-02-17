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
package com.github.workerframework.workermessageprioritization.redistribution.shovel;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.workerframework.workermessageprioritization.rabbitmq.QueuesApi;
import com.github.workerframework.workermessageprioritization.rabbitmq.RabbitManagementApi;
import com.github.workerframework.workermessageprioritization.rabbitmq.ShovelsApi;
import com.github.workerframework.workermessageprioritization.redistribution.config.MessageDistributorConfig;
import com.github.workerframework.workermessageprioritization.redistribution.consumption.ConsumptionTargetCalculator;
import com.github.workerframework.workermessageprioritization.redistribution.consumption.EqualConsumptionTargetCalculator;
import com.github.workerframework.workermessageprioritization.targetcapacitycalculators.K8sTargetQueueCapacityProvider;
import com.rabbitmq.client.ConnectionFactory;

public class ShovelApplication
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ShovelApplication.class);

    public static void main(String[] args) throws IOException, InterruptedException
    {
        final ConnectionFactory connectionFactory = new ConnectionFactory();

        final MessageDistributorConfig messageDistributorConfig = new MessageDistributorConfig();

        LOGGER.info("Read the following configuration: {}", messageDistributorConfig);

        connectionFactory.setHost(messageDistributorConfig.getRabbitMQHost());
        connectionFactory.setUsername(messageDistributorConfig.getRabbitMQUsername());
        connectionFactory.setPassword(messageDistributorConfig.getRabbitMQPassword());
        connectionFactory.setPort(messageDistributorConfig.getRabbitMQPort());
        connectionFactory.setVirtualHost(messageDistributorConfig.getRabbitMQVHost());

        final RabbitManagementApi<QueuesApi> queuesApi = new RabbitManagementApi<>(
            QueuesApi.class,
            messageDistributorConfig.getRabbitMQMgmtUrl(),
            messageDistributorConfig.getRabbitMQMgmtUsername(),
            messageDistributorConfig.getRabbitMQMgmtPassword());

        final RabbitManagementApi<ShovelsApi> shovelsApi = new RabbitManagementApi<>(
            ShovelsApi.class,
            messageDistributorConfig.getRabbitMQMgmtUrl(),
            messageDistributorConfig.getRabbitMQMgmtUsername(),
            messageDistributorConfig.getRabbitMQMgmtPassword());

        final K8sTargetQueueCapacityProvider k8sTargetQueueCapacityProvider = new K8sTargetQueueCapacityProvider(
                messageDistributorConfig.getKubernetesNamespaces(),
                messageDistributorConfig.getKubernetesLabelCacheExpiryMinutes());

        final ConsumptionTargetCalculator consumptionTargetCalculator =
                new EqualConsumptionTargetCalculator(k8sTargetQueueCapacityProvider);

        final ShovelDistributor shovelDistributor = new ShovelDistributor(
            queuesApi,
            shovelsApi,
            consumptionTargetCalculator,
            messageDistributorConfig.getRabbitMQUsername(),
            messageDistributorConfig.getRabbitMQVHost(),
            messageDistributorConfig.getNonRunningShovelTimeoutMilliseconds(),
            messageDistributorConfig.getNonRunningShovelCheckIntervalMilliseconds(),
            messageDistributorConfig.getDistributorRunIntervalMilliseconds());

        shovelDistributor.run();
    }
}
