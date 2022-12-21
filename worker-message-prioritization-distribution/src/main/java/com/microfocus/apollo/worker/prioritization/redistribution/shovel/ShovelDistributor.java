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
package com.microfocus.apollo.worker.prioritization.redistribution.shovel;

import com.microfocus.apollo.worker.prioritization.rabbitmq.Component;
import com.microfocus.apollo.worker.prioritization.rabbitmq.Queue;
import com.microfocus.apollo.worker.prioritization.rabbitmq.QueuesApi;
import com.microfocus.apollo.worker.prioritization.rabbitmq.RabbitManagementApi;
import com.microfocus.apollo.worker.prioritization.rabbitmq.RetrievedShovel;
import com.microfocus.apollo.worker.prioritization.rabbitmq.Shovel;
import com.microfocus.apollo.worker.prioritization.rabbitmq.ShovelsApi;
import com.microfocus.apollo.worker.prioritization.redistribution.DistributorWorkItem;
import com.microfocus.apollo.worker.prioritization.redistribution.MessageDistributor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Set;

public class ShovelDistributor extends MessageDistributor {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(ShovelDistributor.class);
    private final RabbitManagementApi<ShovelsApi> shovelsApi;
    private final long targetQueueMessageLimit;

    public ShovelDistributor(final RabbitManagementApi<QueuesApi> queuesApi,
                                        final RabbitManagementApi<ShovelsApi> shovelsApi, 
                                        final long targetQueueMessageLimit) {

        super(queuesApi);
        this.shovelsApi = shovelsApi;
        this.targetQueueMessageLimit = targetQueueMessageLimit;
    }
    
    public void run() {
        
        final Set<DistributorWorkItem> distributorWorkItems = getDistributorTargets();
        
        final List<RetrievedShovel> retrievedShovels = shovelsApi.getApi().getShovels();
        
        for(final DistributorWorkItem distributorWorkItem : distributorWorkItems) {
            
            final long lastKnownTargetQueueLength = distributorWorkItem.getTargetQueue().getMessages();

            final long totalKnownPendingMessages =
                    distributorWorkItem.getStagingQueues().stream()
                            .map(Queue::getMessages).mapToLong(Long::longValue).sum();

            final long consumptionTarget = targetQueueMessageLimit - lastKnownTargetQueueLength;
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

            if(consumptionTarget <= 0) {
                LOGGER.info("Target queue '{}' consumption target is <= 0, no capacity for new messages, ignoring.",
                        distributorWorkItem.getTargetQueue().getName());
            }
            else {
                for(final Queue sourceQueue: distributorWorkItem.getStagingQueues()) {
                    if(sourceQueue.getMessages() == 0) {
                        LOGGER.info("Source queue '{}' has no messages, ignoring.", 
                                distributorWorkItem.getTargetQueue().getName());

                        continue;
                    }
                    if(retrievedShovels.stream().anyMatch(s -> s.getName().endsWith(sourceQueue.getName()))) {
                        LOGGER.info("Shovel {} already exists, ignoring.", sourceQueue.getName());
                    } else {
                        final Shovel shovel = new Shovel();
//                        shovel.setSrcDeleteAfter(100);
                        shovel.setSrcDeleteAfter((int)sourceQueue.getMessages());
                        shovel.setAckMode("on-confirm");
                        shovel.setSrcQueue(sourceQueue.getName());
                        shovel.setSrcUri("amqp://");
                        shovel.setDestQueue(distributorWorkItem.getTargetQueue().getName());
                        shovel.setDestUri("amqp://");

                        LOGGER.info("Creating shovel {} to consume {} messages.", sourceQueue.getName(), sourceQueue.getMessages());

                        shovelsApi.getApi().putShovel("/", sourceQueue.getName(), 
                                new Component<>("shovel", sourceQueue.getName(), shovel));
                    }
                }
            }            
            
        }
        
    }

}
