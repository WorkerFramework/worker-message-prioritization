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

import com.github.workerframework.workermessageprioritization.rabbitmq.ShovelState;
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
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class ShovelDistributor extends MessageDistributor {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(ShovelDistributor.class);
    
    private static final String ACK_MODE = "on-confirm";

    private final RabbitManagementApi<ShovelsApi> shovelsApi;
    private final ConsumptionTargetCalculator consumptionTargetCalculator;
    private final String rabbitMQVHost;
    private final String rabbitMQUri;
    private final long distributorRunIntervalMilliseconds;
    private final ScheduledExecutorService shovelStateCheckerExecutorService;

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

        this.shovelStateCheckerExecutorService = Executors.newSingleThreadScheduledExecutor();

        shovelStateCheckerExecutorService.scheduleAtFixedRate(
                new ShovelStateChecker(
                        shovelsApi,
                        rabbitMQVHost,
                        nonRunningShovelTimeoutMilliseconds,
                        nonRunningShovelTimeoutCheckIntervalMilliseconds),
                0,
                nonRunningShovelTimeoutCheckIntervalMilliseconds,
                TimeUnit.MILLISECONDS);
    }
    
    public void run() throws InterruptedException {
        try {
            while(true) {
                runOnce();

                try {
                    Thread.sleep(distributorRunIntervalMilliseconds);
                } catch (final InterruptedException e) {
                    LOGGER.warn("Interrupted {}", e.getMessage());
                    throw e;
                }
            }
        } finally {
            shovelStateCheckerExecutorService.shutdownNow();
        }
    }
    
    public void runOnce() {
        
        final Set<DistributorWorkItem> distributorWorkItems = getDistributorWorkItems();

        final List<RetrievedShovel> retrievedShovels;
        try {
            retrievedShovels = shovelsApi.getApi().getShovels();
        } catch (final Exception e) {
            final String errorMessage = String.format(
                    "Failed to get a list of existing shovels, so unable to check if additional shovels need to " +
                            " be created or recreated. Will try again during the next run in %d milliseconds",
                    distributorRunIntervalMilliseconds);

            LOGGER.error(errorMessage, e);

            return;
        }
        
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

                    final String shovelName = queueConsumptionTarget.getKey().getName();

                    final boolean nonTerminatedShovelExists = retrievedShovels.stream()
                            .anyMatch(s -> (s.getState() != ShovelState.TERMINATED) && (s.getName().endsWith(shovelName)));

                    if (nonTerminatedShovelExists) {
                        LOGGER.info("Non-terminated shovel {} already exists, ignoring.", shovelName);
                    } else {
                        // Delete this shovel after all the messages in the staging queue have been consumed OR we have reached the
                        // maximum amount of messages we are allowed to consume from the staging queue (whichever is lower).
                        // This ensures the shovel is always deleted.
                        final long numMessagesInStagingQueue = queueConsumptionTarget.getKey().getMessages();
                        final long maxNumMessagesToConsumeFromStagingQueue = queueConsumptionTarget.getValue();
                        final long srcDeleteAfter = Math.min(numMessagesInStagingQueue, maxNumMessagesToConsumeFromStagingQueue);

                        final Shovel shovel = new Shovel();
                        shovel.setSrcDeleteAfter(srcDeleteAfter);
                        shovel.setAckMode(ACK_MODE);
                        shovel.setSrcQueue(shovelName);
                        shovel.setSrcUri(rabbitMQUri);
                        shovel.setDestQueue(distributorWorkItem.getTargetQueue().getName());
                        shovel.setDestUri(rabbitMQUri);

                        LOGGER.info("Creating (or recreating) shovel named {} with properties {} to consume {} messages",
                                shovelName,
                                shovel,
                                srcDeleteAfter);

                        try {
                            shovelsApi.getApi().putShovel(rabbitMQVHost, shovelName,
                                    new Component<>("shovel",
                                            shovelName,
                                            shovel));
                        } catch (final Exception e) {
                            final String errorMessage = String.format(
                                    "Failed to create (or recreate) shovel named %s with properties %s to consume %s messages. " +
                                    "Will try again during the next run in %d milliseconds (if the shovel is still required then).",
                                    shovelName,
                                    shovel,
                                    srcDeleteAfter,
                                    distributorRunIntervalMilliseconds);

                            LOGGER.error(errorMessage, e);
                        }
                    }
                }
            }
        }
        
    }

}
