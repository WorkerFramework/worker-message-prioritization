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
package com.github.workerframework.workermessageprioritization.redistribution.shovel;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.workerframework.workermessageprioritization.rabbitmq.NodesApi;
import com.github.workerframework.workermessageprioritization.rabbitmq.QueuesApi;
import com.github.workerframework.workermessageprioritization.rabbitmq.RabbitManagementApi;
import com.github.workerframework.workermessageprioritization.rabbitmq.ShovelsApi;
import com.github.workerframework.workermessageprioritization.redistribution.config.MessageDistributorConfig;
import com.github.workerframework.workermessageprioritization.redistribution.consumption.ConsumptionTargetCalculator;
import com.github.workerframework.workermessageprioritization.redistribution.consumption.EqualConsumptionTargetCalculator;
import com.github.workerframework.workermessageprioritization.targetcapacitycalculators.K8sTargetQueueCapacityProvider;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
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

        final RabbitManagementApi<NodesApi> nodesApi = new RabbitManagementApi<>(
                NodesApi.class,
                messageDistributorConfig.getRabbitMQMgmtUrl(),
                messageDistributorConfig.getRabbitMQMgmtUsername(),
                messageDistributorConfig.getRabbitMQMgmtPassword());

        // It is possible that when a shovel gets into a bad state (such as running too long, stuck in a 'starting' state etc.), some
        // operations on it may only work if the request is sent to the same node as the shovel.
        //
        // This cache is used to ensure that any operations on existing shovels (such as delete, restart, recreate) are sent to the
        // same node as the shovel.
        final LoadingCache<String,RabbitManagementApi<ShovelsApi>> nodeSpecificShovelsApiCache = CacheBuilder
                .newBuilder()
                .maximumSize(messageDistributorConfig.getRabbitMQMaxNodeCount())
                .expireAfterAccess(7, TimeUnit.DAYS)
                .build(new CacheLoader<String,RabbitManagementApi<ShovelsApi>>()
                {
                    @Override
                    public RabbitManagementApi<ShovelsApi> load(@Nonnull final String node)
                    {
                        final String nodeSpecificRabbitMqMgmtUrl = NodeSpecificRabbitMqMgmtUrlBuilder.
                                buildNodeSpecificRabbitMqMgmtUrl(node, messageDistributorConfig.getRabbitMQMgmtUrl());

                        LOGGER.info("Creating RabbitManagementApi with URL {}", nodeSpecificRabbitMqMgmtUrl);

                        return new RabbitManagementApi<>(
                                ShovelsApi.class,
                                nodeSpecificRabbitMqMgmtUrl,
                                messageDistributorConfig.getRabbitMQMgmtUsername(),
                                messageDistributorConfig.getRabbitMQMgmtPassword());
                    }
                });

        final K8sTargetQueueCapacityProvider k8sTargetQueueCapacityProvider = new K8sTargetQueueCapacityProvider(
                messageDistributorConfig.getKubernetesNamespaces(),
                messageDistributorConfig.getKubernetesLabelCacheExpiryMinutes());

        final ConsumptionTargetCalculator consumptionTargetCalculator =
                new EqualConsumptionTargetCalculator(k8sTargetQueueCapacityProvider);

        final ShovelDistributor shovelDistributor = new ShovelDistributor(
            queuesApi,
            shovelsApi,
            nodesApi,
            nodeSpecificShovelsApiCache,
            consumptionTargetCalculator,
            messageDistributorConfig.getRabbitMQUsername(),
            messageDistributorConfig.getRabbitMQVHost(),
            messageDistributorConfig.getNonRunningShovelTimeoutMilliseconds(),
            messageDistributorConfig.getNonRunningShovelCheckIntervalMilliseconds(),
            messageDistributorConfig.getShovelRunningTooLongTimeoutMilliseconds(),
            messageDistributorConfig.getShovelRunningTooLongCheckIntervalMilliseconds(),
            messageDistributorConfig.getCorruptedShovelTimeoutMilliseconds(),
            messageDistributorConfig.getCorruptedShovelCheckIntervalMilliseconds(),
            messageDistributorConfig.getDistributorRunIntervalMilliseconds());

        shovelDistributor.run();
    }
}
