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
package com.microfocus.apollo.worker.prioritization.redistribution.lowlevel;

import com.microfocus.apollo.worker.prioritization.rabbitmq.Queue;
import com.microfocus.apollo.worker.prioritization.redistribution.ConsumptionTargetCalculator;
import com.microfocus.apollo.worker.prioritization.redistribution.DistributorWorkItem;
import com.rabbitmq.client.Connection;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Moves messages from staging queues to the target queue
 */
public class MessageMover {
    
    private final Connection connection;
    private final DistributorWorkItem distributorWorkItem;
    private final ConsumptionTargetCalculator consumptionTargetCalculator;
    private Set<StagingQueueTargetQueuePair> stagingQueueTargetQueuePairs = new HashSet<>();

    public MessageMover(final Connection connection,
                        final DistributorWorkItem distributorWorkItem,
                        final ConsumptionTargetCalculator consumptionTargetCalculator) {
        this.connection = connection;
        this.distributorWorkItem = distributorWorkItem;
        this.consumptionTargetCalculator = consumptionTargetCalculator;
    }
    
    public void start() throws IOException {

        final Map<Queue, Long> consumptionTargets = consumptionTargetCalculator
                .calculateConsumptionTargets(distributorWorkItem);
        
        final long overallConsumptionTarget = consumptionTargets.values().stream().mapToLong(Long::longValue).sum();
        
        if(overallConsumptionTarget <= 0) {
            return;
        }
        
        for(final Queue stagingQueue: distributorWorkItem.getStagingQueues()) {
            
            final Long consumptionTarget = consumptionTargets.get(stagingQueue);
            
            var stagingQueueTargetQueuePair = 
                    new StagingQueueTargetQueuePair(connection, 
                            distributorWorkItem.getTargetQueue(), stagingQueue,
                            consumptionTarget);

            stagingQueueTargetQueuePairs.add(stagingQueueTargetQueuePair);
            stagingQueueTargetQueuePair.start();
        }
    }
}
