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
package com.github.workerframework.workermessageprioritization.redistribution.consumption;

import com.github.workerframework.workermessageprioritization.rabbitmq.Queue;
import com.github.workerframework.workermessageprioritization.redistribution.DistributorWorkItem;
import com.github.workerframework.workermessageprioritization.targetcapacitycalculators.TargetQueueCapacityProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.stream.Collectors;

/**
 * Attempts to send an equal number of message from each staging queue to the target queue
 */
public class EqualConsumptionTargetCalculator implements ConsumptionTargetCalculator {

    private static final Logger LOGGER = LoggerFactory.getLogger(EqualConsumptionTargetCalculator.class);
    private final TargetQueueCapacityProvider targetQueueCapacityProvider;

    public EqualConsumptionTargetCalculator(final TargetQueueCapacityProvider targetQueueCapacityProvider) {

        this.targetQueueCapacityProvider = targetQueueCapacityProvider;
    }
    
    @Override
    public Map<Queue, Long> calculateConsumptionTargets(final DistributorWorkItem distributorWorkItem) {
        
        final var targetQueueCapacity = targetQueueCapacityProvider.get(distributorWorkItem.getTargetQueue());
        
        final long lastKnownTargetQueueLength = distributorWorkItem.getTargetQueue().getMessages();

        final long totalKnownPendingMessages =
                distributorWorkItem.getStagingQueues().stream()
                        .map(Queue::getMessages).mapToLong(Long::longValue).sum();

        final long consumptionTarget = targetQueueCapacity - lastKnownTargetQueueLength;
        final long sourceQueueConsumptionTarget;
        if(distributorWorkItem.getStagingQueues().isEmpty()) {
            sourceQueueConsumptionTarget = 0;
        }
        else {
            sourceQueueConsumptionTarget = (long) Math.ceil((double)consumptionTarget /
                    distributorWorkItem.getStagingQueues().size());
        }

        LOGGER.info("TargetQueue {}, {} messages, SourceQueues {}, {} messages, " +
                        "Overall consumption target: {}, Individual Source Queue consumption target: {}",
                distributorWorkItem.getTargetQueue().getName(), lastKnownTargetQueueLength,
                (long) distributorWorkItem.getStagingQueues().size(), totalKnownPendingMessages,
                consumptionTarget, sourceQueueConsumptionTarget);
        
        return distributorWorkItem.getStagingQueues().stream()
                .collect(Collectors.toMap(q -> q, q-> sourceQueueConsumptionTarget));
        
    }
}
