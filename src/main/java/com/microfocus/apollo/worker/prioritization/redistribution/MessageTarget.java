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
package com.microfocus.apollo.worker.prioritization.redistribution;

import com.microfocus.apollo.worker.prioritization.management.Queue;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.ShutdownSignalException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;


public class MessageTarget {
    private static final Logger LOGGER = LoggerFactory.getLogger(MessageTarget.class);
    private final long targetQueueCapacity;
    private final ConnectionFactory connectionFactory;
    private final ConcurrentHashMap<String, MessageSource> messageSources = new ConcurrentHashMap<>();
    private final Queue targetQueue;
    private ShutdownSignalException shutdownSignalException = null;
    
    public MessageTarget(final long targetQueueCapacity, final ConnectionFactory connectionFactory, final Queue targetQueue) {
        this.targetQueueCapacity = targetQueueCapacity;
        this.connectionFactory = connectionFactory;
        this.targetQueue = targetQueue;
    }
    
    public String getTargetQueueName() {
        return targetQueue.getName();
    }
    
    private void updateTargetQueueMetadata(final Queue targetQueue) {
        this.targetQueue.setMessages(targetQueue.getMessages());
    }
    
    private void updateMessageSources(final Set<Queue> messageSourceQueues) {

        for(final MessageSource cancelledMessageSource: messageSources.values().stream()
                .filter(MessageSource::isCancelled).collect(Collectors.toList())) {
            
            messageSources.remove(cancelledMessageSource.getSourceQueue().getName());
            
        }
        
        for(final Queue messageSourceQueue: messageSourceQueues) {
            messageSources.computeIfAbsent(messageSourceQueue.getName(), 
                    k -> {
                        final MessageSource m = new MessageSource(connectionFactory, messageSourceQueue, this);
                        try {
                            m.init();
                        } catch (IOException | TimeoutException e) {
                            throw new RuntimeException(e);
                        }
                        return m;
                    }
                );
        }
    }
    
    public void run(final Queue targetQueue, final Set<Queue> messageSourceQueues) {

        updateTargetQueueMetadata(targetQueue);
        updateMessageSources(messageSourceQueues);
        if(messageSources.isEmpty()) {
            return;
        }
        
        final long lastKnownTargetQueueLength = targetQueue.getMessages();

        final long totalKnownPendingMessages =
                messageSources.values().stream().map(ms 
                        -> ms.getSourceQueue().getMessages()).mapToLong(Long::longValue).sum();

        final long consumptionTarget = targetQueueCapacity - lastKnownTargetQueueLength;
        final long sourceQueueConsumptionTarget;
        if(messageSources.isEmpty()) {
            sourceQueueConsumptionTarget = 0;
        }
        else {
            sourceQueueConsumptionTarget = (long) Math.ceil((double)consumptionTarget / messageSources.size());
        }

        LOGGER.info("TargetQueue {}, {} messages, SourceQueues {}, {} messages, " +
                        "Overall consumption target: {}, Individual Source Queue consumption target: {}",
                targetQueue.getName(), lastKnownTargetQueueLength,
                (long) messageSources.size(), totalKnownPendingMessages,
                consumptionTarget, sourceQueueConsumptionTarget);

        if(consumptionTarget <= 0) {
            LOGGER.info("Target queue '{}' consumption target is <= 0, no capacity for new messages, ignoring.", targetQueue.getName());
        }
        else {
            for(final MessageSource messageSource: messageSources.values()) {
                messageSource.run(sourceQueueConsumptionTarget);
            }
        }

    }
    
    public ShutdownSignalException getShutdownSignalException() {
        return shutdownSignalException;
    }
}
