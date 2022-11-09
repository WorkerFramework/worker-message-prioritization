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
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConfirmListener;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ShutdownSignalException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;

public class MessageTarget {
    private static final Logger LOGGER = LoggerFactory.getLogger(MessageTarget.class);
    private final long targetQueueCapacity;
    private final Connection connection;
    private Channel channel;
    private final Set<String> activeQueueConsumers = ConcurrentHashMap.newKeySet();
    private final Queue targetQueue;
    private ShutdownSignalException shutdownSignalException = null;
    final ConcurrentNavigableMap<Long, Long> outstandingConfirms = new ConcurrentSkipListMap<>();
    
    public MessageTarget(final long targetQueueCapacity, final Connection connection, final Queue targetQueue) {
        this.targetQueueCapacity = targetQueueCapacity;
        this.connection = connection;
        this.targetQueue = targetQueue;
    }
    
    public String getTargetQueueName() {
        return targetQueue.getName();
    }
    
    public void initialise() throws IOException {
        this.channel = connection.createChannel();
        channel.confirmSelect();
        channel.addConfirmListener(new ConfirmListener() {
            @Override
            public void handleAck(long deliveryTag, boolean multiple) throws IOException {
                if (multiple) {
                    final ConcurrentNavigableMap<Long, Long> confirmed = outstandingConfirms.headMap(
                            deliveryTag, true
                    );

                    for(final Long messageDeliveryTagToAck: confirmed.values()) {
                        try {
                            channel.basicAck(messageDeliveryTagToAck, true);
                        }
                        catch (final IOException e) {
                            //TODO Consider allowing a retry limit before escalating and removing this messageSource
                            LOGGER.error("Exception ack'ing '{}' {}",
                                    messageDeliveryTagToAck,
                                    e.toString());
                        }
                    }

                    confirmed.clear();
                } else {
                    channel.basicAck(outstandingConfirms.get(deliveryTag), false);
                    outstandingConfirms.remove(deliveryTag);
                }
            }

            @Override
            public void handleNack(long deliveryTag, boolean multiple) throws IOException {
                if (multiple) {
                    final ConcurrentNavigableMap<Long, Long> confirmed = outstandingConfirms.headMap(
                            deliveryTag, true
                    );

                    for(final Long messageDeliveryTagToAck: confirmed.values()) {
                        try {
                            channel.basicNack(messageDeliveryTagToAck, true, true);
                        }
                        catch (final IOException e) {
                            //TODO Consider allowing a retry limit before escalating and removing this messageSource
                            LOGGER.error("Exception ack'ing '{}' {}",
                                    messageDeliveryTagToAck,
                                    e.toString());
                        }
                    }

                    confirmed.clear();
                } else {
                    channel.basicNack(outstandingConfirms.get(deliveryTag), false, true);
                    outstandingConfirms.remove(deliveryTag);
                }
            }
        });
    }
    
    public void run(final Queue targetQueue, final Set<Queue> messageSources) {

        updateQueueMetadata(targetQueue);
        
        final long lastKnownTargetQueueLength = targetQueue.getMessages();

        final long totalKnownPendingMessages =
                messageSources.stream().map(Queue::getMessages).mapToLong(Long::longValue).sum();

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
            for(final Queue messageSource: messageSources) {
                wireup(messageSource, sourceQueueConsumptionTarget);
            }
        }

    }
    
    public void updateQueueMetadata(final Queue queue) {
        this.targetQueue.setMessages(queue.getMessages());
    }
        
    private void wireup(final Queue messageSource, final long consumptionLimit) {

        if(activeQueueConsumers.contains(messageSource.getName())) {
            LOGGER.info("Source queue '{}' is still active, ignoring wireup request.", messageSource.getName());
            return;
        }

        try {
            final AtomicInteger messageCount = new AtomicInteger(0);
            activeQueueConsumers.add(messageSource.getName());
            channel.basicConsume(messageSource.getName(),
                    (consumerTag, message) -> {

                        final long nextPublishSequenceNumber = channel.getNextPublishSeqNo();
                        outstandingConfirms.put(nextPublishSequenceNumber, message.getEnvelope().getDeliveryTag());
                        try {
                            channel.basicPublish("",
                                    targetQueue.getName(), message.getProperties(), message.getBody());
                        }
                        catch (final IOException e) {
                            //TODO Consider allowing a retry limit before escalating and stopping this MessageTarget
                            LOGGER.error("Exception publishing to '{}' {}", targetQueue.getName(),
                                    e.toString());
                        }
                        messageCount.incrementAndGet();

                        if(messageCount.get() > consumptionLimit) {
                            LOGGER.trace("Consumption target '{}' reached for '{}'.", consumptionLimit, 
                                    messageSource.getName());
                            channel.basicCancel(consumerTag);
                        }
                    },
                    consumerTag -> {
                        //Stop tracking that we are consuming from the consumerTag queue
                        activeQueueConsumers.remove(messageSource.getName());
                    },
                    (consumerTag, sig) -> {
                        //Connection lost, give up
                        shutdownSignalException = sig;
                    });
        } catch (final IOException e) {
            LOGGER.error("Exception registering consumers from '{}'", messageSource);
            activeQueueConsumers.remove(messageSource.getName());
        }

    }

    public ShutdownSignalException getShutdownSignalException() {
        return shutdownSignalException;
    }
}
