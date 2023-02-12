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
package com.github.workerframework.workermessageprioritization.redistribution;

import com.github.workerframework.workermessageprioritization.rabbitmq.Queue;
import com.github.workerframework.workermessageprioritization.rabbitmq.QueuesApi;
import com.github.workerframework.workermessageprioritization.rabbitmq.RabbitManagementApi;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class MessageDistributor {

    private static final Logger LOGGER = LoggerFactory.getLogger(MessageDistributor.class);

    public static final String LOAD_BALANCED_INDICATOR = "»";

    private final RabbitManagementApi<QueuesApi> queuesApi;

    public MessageDistributor(final RabbitManagementApi<QueuesApi> queuesApi) {
        this.queuesApi = queuesApi;
    }
    
    protected Set<DistributorWorkItem> getDistributorWorkItems() {
        final List<Queue> queues = queuesApi.getApi().getQueues();

        LOGGER.debug("Read the following list of queues from the RabbitMQ API: {}", queues);

        final Set<DistributorWorkItem> distributorWorkItems = new HashSet<>();
        
        for(final Queue targetQueue: queues.stream().filter(q -> !q.getName().contains(LOAD_BALANCED_INDICATOR))
                .collect(Collectors.toSet())) {

            final Set<Queue> stagingQueues = queues.stream()
                    .filter(q ->
                            q.getMessages() > 0 &&
                                    q.getName().startsWith(targetQueue.getName() + LOAD_BALANCED_INDICATOR))
                    .collect(Collectors.toSet());
            
            if(stagingQueues.isEmpty()) {
                continue;
            }
            
            LOGGER.debug("Creating a new DistributorWorkItem for target queue: {} and staging queues: {}", targetQueue, stagingQueues);
            
            distributorWorkItems.add(new DistributorWorkItem(targetQueue, stagingQueues));
            
        }
        return distributorWorkItems;
    }
}
