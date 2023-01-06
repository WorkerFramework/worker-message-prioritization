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
import com.microfocus.apollo.worker.prioritization.redistribution.consumption.ConsumptionTargetCalculator;
import com.microfocus.apollo.worker.prioritization.redistribution.DistributorWorkItem;
import com.microfocus.apollo.worker.prioritization.redistribution.MessageDistributor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class ShovelDistributor extends MessageDistributor {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(ShovelDistributor.class);
    private final RabbitManagementApi<ShovelsApi> shovelsApi;
    private final ConsumptionTargetCalculator consumptionTargetCalculator;

    public ShovelDistributor(
            final RabbitManagementApi<QueuesApi> queuesApi,
            final RabbitManagementApi<ShovelsApi> shovelsApi,
            final ConsumptionTargetCalculator consumptionTargetCalculator) {

        super(queuesApi);
        this.shovelsApi = shovelsApi;
        this.consumptionTargetCalculator = consumptionTargetCalculator;
    }
    
    public void run() {
        while(true) {
            runOnce();
        }
    }
    
    public void runOnce() {
        
        final Set<DistributorWorkItem> distributorWorkItems = getDistributorWorkItems();
        
        final List<RetrievedShovel> retrievedShovels = shovelsApi.getApi().getShovels();
        
        for(final DistributorWorkItem distributorWorkItem : distributorWorkItems) {
            
            final Map<Queue, Long> consumptionTarget = consumptionTargetCalculator.calculateConsumptionTargets(distributorWorkItem);
            
            final long overallConsumptionTarget = consumptionTarget.values().stream().mapToLong(Long::longValue).sum();
            
            if(overallConsumptionTarget <= 0) {
                LOGGER.info("Target queue '{}' consumption target is <= 0, no capacity for new messages, ignoring.",
                        distributorWorkItem.getTargetQueue().getName());
            }
            else {
                for(final Map.Entry<Queue, Long> queueConsumptionTarget: consumptionTarget.entrySet()) {
//                    if(sourceQueue.getMessages() == 0) {
//                        LOGGER.info("Source queue '{}' has no messages, ignoring.", 
//                                distributorWorkItem.getTargetQueue().getName());
//
//                        continue;
//                    }
                    if(retrievedShovels.stream().anyMatch(s -> s.getName()
                            .endsWith(queueConsumptionTarget.getKey().getName()))) {
                        LOGGER.info("Shovel {} already exists, ignoring.", queueConsumptionTarget.getKey().getName());
                    } else {
                        final Shovel shovel = new Shovel();
//                        shovel.setSrcDeleteAfter(100);
                        shovel.setSrcDeleteAfter((int)queueConsumptionTarget.getKey().getMessages());
                        shovel.setAckMode("on-confirm");
                        shovel.setSrcQueue(queueConsumptionTarget.getKey().getName());
                        shovel.setSrcUri("amqp://");
                        shovel.setDestQueue(distributorWorkItem.getTargetQueue().getName());
                        shovel.setDestUri("amqp://");

                        LOGGER.info("Creating shovel {} to consume {} messages.", 
                                queueConsumptionTarget.getKey().getName(), 
                                queueConsumptionTarget.getKey().getMessages());

                        shovelsApi.getApi().putShovel("/", queueConsumptionTarget.getKey().getName(),
                                new Component<>("shovel",
                                        queueConsumptionTarget.getKey().getName(),
                                        shovel));
                    }
                }
            }            
            
        }
        
    }

}
