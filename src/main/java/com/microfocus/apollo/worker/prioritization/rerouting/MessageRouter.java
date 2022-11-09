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
package com.microfocus.apollo.worker.prioritization.rerouting;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.hpe.caf.worker.document.model.Document;
import com.hpe.caf.worker.document.model.Response;
import com.hpe.caf.worker.document.model.ResponseQueue;
import com.microfocus.apollo.worker.prioritization.management.Queue;
import com.microfocus.apollo.worker.prioritization.management.QueuesApi;
import com.microfocus.apollo.worker.prioritization.management.RabbitManagementApi;
import com.microfocus.apollo.worker.prioritization.rerouting.mutators.QueueNameMutator;
import com.microfocus.apollo.worker.prioritization.rerouting.mutators.TenantQueueNameMutator;
import com.microfocus.apollo.worker.prioritization.rerouting.mutators.WorkflowQueueNameMutator;
import com.rabbitmq.client.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class MessageRouter {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(MessageRouter.class);
    
    public static final String LOAD_BALANCED_INDICATOR = "»";

    private static final Map<String, Object>arguments = Stream.of(new Object[][]{
                    {"queue-mode", "lazy"},
                    {"x-max-priority", "5"}
            })
            .collect(Collectors.toMap(d -> (String) d[0], d -> d[1]));
    
    private final List<QueueNameMutator> queueNameMutators = Stream.of(
            new TenantQueueNameMutator(), new WorkflowQueueNameMutator()).collect(Collectors.toList());
    
    private final LoadingCache<String, Queue> queuesCache;
    private final HashSet<String> declaredQueues = new HashSet<>();
    private final Channel channel;
    private final long targetQueueMessageLimit;

    public MessageRouter(final RabbitManagementApi<QueuesApi> queuesApi, final String vhost, final Channel channel,
                         final long targetQueueMessageLimit) {
        this.targetQueueMessageLimit = targetQueueMessageLimit;

        this.queuesCache = CacheBuilder.newBuilder()
                .expireAfterWrite(1, TimeUnit.MINUTES)
                .build(new CacheLoader<String, Queue>() {
                    @Override
                    public Queue load(@Nonnull final String queueName) throws Exception {
                        return queuesApi.getApi().getQueue(vhost, queueName);
                    }
                });
        
        this.channel = channel;
    }
    
    public void route(final Document document) {
        final Response response = document.getTask().getResponse();
        final String originalQueueName = response.getSuccessQueue().getName();
        
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
                ensureQueueExists(response.getSuccessQueue().getName());
            } catch (final IOException e) {
                LOGGER.error("Unable to verify the new target queue '{}' exists, reverting to original queue. {}",
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

        return queue.getMessages() > targetQueueMessageLimit;
    }
    
    private void ensureQueueExists(final String reroutedQueueName) 
            throws IOException {
        
        if(declaredQueues.contains(reroutedQueueName)) {
            return;
        }
        
        //Durable lazy queue
        //This is a basic implementation for the POC, we may want to retrieve the definition of the originalQueue and
        //use that to ensure the lazy queue has the same configuration.
        channel.queueDeclare(reroutedQueueName, true, false, false, arguments);

        declaredQueues.add(reroutedQueueName);
    }
}
