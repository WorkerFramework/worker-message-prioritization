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

import com.github.workerframework.workermessageprioritization.rabbitmq.Queue;
import com.github.workerframework.workermessageprioritization.redistribution.consumption.ConsumptionTargetCalculator;
import com.github.workerframework.workermessageprioritization.rabbitmq.QueuesApi;
import com.github.workerframework.workermessageprioritization.rabbitmq.RabbitManagementApi;
import com.github.workerframework.workermessageprioritization.redistribution.DistributorWorkItem;
import com.github.workerframework.workermessageprioritization.redistribution.MessageDistributor;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeoutException;

public class LowLevelDistributor extends MessageDistributor {

    private static final Logger LOGGER = LoggerFactory.getLogger(LowLevelDistributor.class);

    private final HashMap<String, StagingQueueTargetQueuePair> existingStagingQueueTargetQueuePairs = new HashMap<>();
    private final ConsumptionTargetCalculator consumptionTargetCalculator;
    private final StagingTargetPairProvider stagingTargetPairProvider;
    private final ConnectionFactory connectionFactory;
    private final String connectionDetails;
    private final long distributorRunIntervalMilliseconds;

    public LowLevelDistributor(final RabbitManagementApi<QueuesApi> queuesApi,
                               final ConnectionFactory connectionFactory,
                               final ConsumptionTargetCalculator consumptionTargetCalculator,
                               final StagingTargetPairProvider stagingTargetPairProvider,
                               final long distributorRunIntervalMilliseconds) {
        super(queuesApi);
        this.connectionFactory = connectionFactory;
        this.connectionDetails = String.format(
            "Host: %s, Port: %s, Virtual Host: %s, SSL: %s",
            connectionFactory.getHost(),
            connectionFactory.getPort(),
            connectionFactory.getVirtualHost(),
            connectionFactory.isSSL());
        this.consumptionTargetCalculator = consumptionTargetCalculator;
        this.stagingTargetPairProvider = stagingTargetPairProvider;
        this.distributorRunIntervalMilliseconds = distributorRunIntervalMilliseconds;
    }
    
    public void run() throws IOException, TimeoutException, InterruptedException {

        try(final Connection connection = connectionFactory.newConnection()) {

            LOGGER.info(String.format("Successfully connected to RabbitMQ. Connection details: %s", connectionDetails));

            while (connection.isOpen()) {
                runOnce(connection);

                try {
                    Thread.sleep(distributorRunIntervalMilliseconds);
                } catch (final InterruptedException e) {
                    LOGGER.warn("Interrupted {}", e.getMessage());
                    throw e;
                }

            }
        } catch (final IOException ioException) {
            LOGGER.error(String.format("Failed to connect to RabbitMQ. Connection details: %s", connectionDetails), ioException);
            throw ioException;
        }
        
    }
    
    public void runOnce(final Connection connection) throws IOException {
        final Set<DistributorWorkItem> distributorWorkItems = getDistributorWorkItems();

        for (final DistributorWorkItem distributorWorkItem : distributorWorkItems) {
            final Map<Queue, Long> consumptionTargets = consumptionTargetCalculator.calculateConsumptionTargets(distributorWorkItem);
            final Set<StagingQueueTargetQueuePair> stagingTargetPairs =
                    stagingTargetPairProvider.provideStagingTargetPairs(
                            connection, distributorWorkItem, consumptionTargets);

            for (final StagingQueueTargetQueuePair stagingTargetPair : stagingTargetPairs) {
                if (existingStagingQueueTargetQueuePairs.containsKey(stagingTargetPair.getIdentifier())) {
                    final StagingQueueTargetQueuePair existingStagingQueueTargetQueuePair =
                            existingStagingQueueTargetQueuePairs.get(stagingTargetPair.getIdentifier());

                    if (!existingStagingQueueTargetQueuePair.isCompleted()) {
                        LOGGER.warn("Existing StagingQueueTargetQueuePair '{}' was still running",
                                existingStagingQueueTargetQueuePair.getIdentifier());
                        continue;
                    } else {
                        if (existingStagingQueueTargetQueuePair.getShutdownSignalException() != null) {
                            LOGGER.error("Exiting as '{}' recorded a shutdown exception.",
                                    existingStagingQueueTargetQueuePair.getIdentifier());
                            return;
                        }
                        existingStagingQueueTargetQueuePairs.remove(stagingTargetPair.getIdentifier());
                    }
                }
                existingStagingQueueTargetQueuePairs
                        .put(stagingTargetPair.getIdentifier(), stagingTargetPair);
                stagingTargetPair.startConsuming();
            }

        }
    }
}
