/*
 * Copyright 2022-2022 Micro Focus or one of its affiliates.
 *
 * The only warranties for products and services of Micro Focus and its
 * affiliates and licensors ("Micro Focus") are set forth in the express
 * warranty statements accompanying such products and services. Nothing
 * herein should be construed as constituting an additional warranty.
 * Micro Focus shall not be liable for technical or editorial errors or
 * omissions contained herein. The information contained herein is subject
 * to change without notice.
 *
 * Contains Confidential Information. Except as specifically indicated
 * otherwise, a valid license is required for possession, use or copying.
 * Consistent with FAR 12.211 and 12.212, Commercial Computer Software,
 * Computer Software Documentation, and Technical Data for Commercial
 * Items are licensed to the U.S. Government under vendor's standard
 * commercial license.
 */
package com.github.workerframework.workermessageprioritization.redistribution.lowlevel;

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
import java.util.Set;
import java.util.concurrent.TimeoutException;

public class LowLevelDistributor extends MessageDistributor {

    private static final Logger LOGGER = LoggerFactory.getLogger(LowLevelDistributor.class);

    private final HashMap<String, StagingQueueTargetQueuePair> existingStagingQueueTargetQueuePairs = new HashMap<>();
    private final ConsumptionTargetCalculator consumptionTargetCalculator;
    private final StagingTargetPairProvider stagingTargetPairProvider;
    private final ConnectionFactory connectionFactory;

    public LowLevelDistributor(final RabbitManagementApi<QueuesApi> queuesApi,
                               final ConnectionFactory connectionFactory,
                               final ConsumptionTargetCalculator consumptionTargetCalculator,
                               final StagingTargetPairProvider stagingTargetPairProvider) {
        super(queuesApi);
        this.connectionFactory = connectionFactory;
        this.consumptionTargetCalculator = consumptionTargetCalculator;
        this.stagingTargetPairProvider = stagingTargetPairProvider;
    }
    
    public void run() throws IOException, TimeoutException, InterruptedException {
        
        try(final Connection connection = connectionFactory.newConnection()) {
            while (connection.isOpen()) {
                runOnce(connection);

                try {
                    Thread.sleep(1000 * 10);
                } catch (final InterruptedException e) {
                    LOGGER.warn("Interrupted {}", e.getMessage());
                    throw e;
                }

            }
        }
        
    }
    
    public void runOnce(final Connection connection) throws IOException {
        final Set<DistributorWorkItem> distributorWorkItems = getDistributorWorkItems();

        for (final DistributorWorkItem distributorWorkItem : distributorWorkItems) {
            final var consumptionTargets = consumptionTargetCalculator.calculateConsumptionTargets(distributorWorkItem);
            final var stagingTargetPairs =
                    stagingTargetPairProvider.provideStagingTargetPairs(
                            connection, distributorWorkItem, consumptionTargets);

            for (final var stagingTargetPair : stagingTargetPairs) {
                if (existingStagingQueueTargetQueuePairs.containsKey(stagingTargetPair.getIdentifier())) {
                    final var existingStagingQueueTargetQueuePair =
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
