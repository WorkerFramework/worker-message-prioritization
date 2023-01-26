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

import com.github.workerframework.workermessageprioritization.redistribution.MessageDistributor;
import com.github.workerframework.workermessageprioritization.redistribution.consumption.ConsumptionTargetCalculator;
import com.github.workerframework.workermessageprioritization.rabbitmq.Component;
import com.github.workerframework.workermessageprioritization.rabbitmq.Queue;
import com.github.workerframework.workermessageprioritization.rabbitmq.QueuesApi;
import com.github.workerframework.workermessageprioritization.rabbitmq.RabbitManagementApi;
import com.github.workerframework.workermessageprioritization.rabbitmq.RetrievedShovel;
import com.github.workerframework.workermessageprioritization.rabbitmq.Shovel;
import com.github.workerframework.workermessageprioritization.rabbitmq.ShovelsApi;
import com.github.workerframework.workermessageprioritization.redistribution.DistributorWorkItem;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class ShovelDistributor extends MessageDistributor {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(ShovelDistributor.class);
    
    private static final String ACK_MODE = "on-confirm";

    private final RabbitManagementApi<ShovelsApi> shovelsApi;
    private final ConsumptionTargetCalculator consumptionTargetCalculator;
    private final String rabbitMQVHost;
    private final String rabbitMQUri;
    private final long distributorRunIntervalMilliseconds;

    public ShovelDistributor(
            final RabbitManagementApi<QueuesApi> queuesApi,
            final RabbitManagementApi<ShovelsApi> shovelsApi,
            final ConsumptionTargetCalculator consumptionTargetCalculator,
            final String rabbitMQUsername,
            final String rabbitMQVHost,
            final long nonRunningShovelTimeoutMilliseconds,
            final long nonRunningShovelTimeoutCheckIntervalMilliseconds,
            final long distributorRunIntervalMilliseconds) throws UnsupportedEncodingException {

        super(queuesApi);
        this.shovelsApi = shovelsApi;
        this.consumptionTargetCalculator = consumptionTargetCalculator;
        this.rabbitMQVHost = rabbitMQVHost;
        this.rabbitMQUri = String.format(
            "amqp://%s@/%s", rabbitMQUsername, URLEncoder.encode(this.rabbitMQVHost, StandardCharsets.UTF_8.toString()));
        this.distributorRunIntervalMilliseconds = distributorRunIntervalMilliseconds;

        Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(
            new ShovelStateChecker(shovelsApi, rabbitMQVHost, nonRunningShovelTimeoutMilliseconds),
            0,
            nonRunningShovelTimeoutCheckIntervalMilliseconds,
            TimeUnit.MILLISECONDS);
    }
    
    public void run() throws InterruptedException {
        while(true) {
            runOnce();

            try {
                Thread.sleep(distributorRunIntervalMilliseconds);
            } catch (final InterruptedException e) {
                LOGGER.warn("Interrupted {}", e.getMessage());
                throw e;
            }
        }
    }
    
    public void runOnce() {
        
        final Set<DistributorWorkItem> distributorWorkItems = getDistributorWorkItems();
        
        final List<RetrievedShovel> retrievedShovels = shovelsApi.getApi().getShovels();
        
        LOGGER.debug("Read the following list of shovels from the RabbitMQ API: {}", retrievedShovels);
        
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
                        // Delete this shovel after all the messages in the staging queue have been consumed OR we have reached the
                        // maximum amount of messages we are allowed to consume from the staging queue (whichever is lower).
                        // This ensures the shovel is always deleted.
                        final long numMessagesInStagingQueue = queueConsumptionTarget.getKey().getMessages();
                        final long maxNumMessagesToConsumeFromStagingQueue = queueConsumptionTarget.getValue();
                        final long srcDeleteAfter = Math.min(numMessagesInStagingQueue, maxNumMessagesToConsumeFromStagingQueue);
 
                        final String stagingQueue = queueConsumptionTarget.getKey().getName();
                        final String targetQueue = distributorWorkItem.getTargetQueue().getName();

                        final Shovel shovel = new Shovel();
                        shovel.setSrcDeleteAfter(srcDeleteAfter);
                        shovel.setAckMode(ACK_MODE);
                        shovel.setSrcQueue(stagingQueue);
                        shovel.setSrcUri(rabbitMQUri);
                        shovel.setDestQueue(targetQueue);
                        shovel.setDestUri(rabbitMQUri);

                        LOGGER.info("Creating shovel named {} with properties {} to consume {} messages",
                                    stagingQueue,
                                    shovel,
                                    srcDeleteAfter);

                        shovelsApi.getApi().putShovel(rabbitMQVHost, stagingQueue,
                                                      new Component<>("shovel",
                                                                      stagingQueue,
                                                                      shovel));                      
                    }
                }
            }
        }
        
    }

}
