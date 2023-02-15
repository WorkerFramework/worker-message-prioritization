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
package com.github.workerframework.workermessageprioritization.rerouting;

import com.github.workerframework.workermessageprioritization.rerouting.mutators.QueueNameMutator;
import com.github.workerframework.workermessageprioritization.rerouting.mutators.WorkflowQueueNameMutator;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.hpe.caf.worker.document.model.Document;
import com.hpe.caf.worker.document.model.Response;
import com.hpe.caf.worker.document.model.ResponseQueue;
import com.github.workerframework.workermessageprioritization.rabbitmq.Queue;
import com.github.workerframework.workermessageprioritization.rabbitmq.QueuesApi;
import com.github.workerframework.workermessageprioritization.rabbitmq.RabbitManagementApi;
import com.github.workerframework.workermessageprioritization.rerouting.mutators.TenantQueueNameMutator;
import com.github.workerframework.workermessageprioritization.targetcapacitycalculators.TargetQueueCapacityProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import com.github.workerframework.workermessageprioritization.rerouting.reroutedeciders.RerouteDecider;

public class MessageRouter {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(MessageRouter.class);
    
    public static final String LOAD_BALANCED_INDICATOR = "»";
    
    private final List<QueueNameMutator> queueNameMutators = Stream.of(
            new TenantQueueNameMutator(), new WorkflowQueueNameMutator()).collect(Collectors.toList());
    private final LoadingCache<String, Queue> queuesCache;
    private final StagingQueueCreator stagingQueueCreator;
    private final RerouteDecider rerouteDecider;

    public MessageRouter(final RabbitManagementApi<QueuesApi> queuesApi,
                         final String vhost,
                         final StagingQueueCreator stagingQueueCreator,
                         final RerouteDecider rerouteDecider) {

        this.stagingQueueCreator = stagingQueueCreator;

        this.rerouteDecider = rerouteDecider;
        
        this.queuesCache = CacheBuilder.newBuilder()
                .expireAfterWrite(1, TimeUnit.MINUTES)
                .build(new CacheLoader<String, Queue>() {
                    @Override
                    public Queue load(@Nonnull final String queueName) throws Exception {
                        return queuesApi.getApi().getQueue(vhost, queueName);
                    }
                });
    }
    
    public void route(final Document document) {
        final Response response = document.getTask().getResponse();
        final String originalQueueName = response.getSuccessQueue().getName();

        final Queue originalQueue;
        try {
             originalQueue = queuesCache.get(originalQueueName);
        } catch (final ExecutionException e) {
            LOGGER.error(
                    "Unable to retrieve the definition of original queue '{}' exists, reverting to original queue. {}",
                    response.getSuccessQueue().getName(), e);

            response.getSuccessQueue().set(originalQueueName);
            return;
        }

        if(shouldReroute(response.getSuccessQueue())) {
            
            response.getSuccessQueue().set(originalQueueName + LOAD_BALANCED_INDICATOR);
            
            for(final QueueNameMutator queueNameMutator: queueNameMutators) {
                queueNameMutator.mutateSuccessQueueName(document);
            }
            
            if(response.getSuccessQueue().getName().equals(originalQueueName + LOAD_BALANCED_INDICATOR)) {
                //No meaningful change was made, revert to using the original queue name
                response.getSuccessQueue().set(originalQueueName);
                return;
            }

            try {
                stagingQueueCreator.createStagingQueue(originalQueue, response.getSuccessQueue().getName());
            } catch (final IOException e) {
                LOGGER.error(
                        "Unable to create the new staging queue '{}' exists, reverting to original queue. {}",
                        response.getSuccessQueue().getName(), e);

                response.getSuccessQueue().set(originalQueueName);
            }
            
        }

    }
    
    private boolean shouldReroute(final ResponseQueue successQueue) {
        final Queue queue;
        try {
            queue = queuesCache.get(successQueue.getName());
        } catch (final ExecutionException e) {
            LOGGER.error("Could not retrieve queue stats for {} to determine need for reroute.\n{}", 
                    successQueue.getName(), e.getCause().toString());
            
            return false;
        }

        return rerouteDecider.shouldReroute(queue);
    }
    
}
