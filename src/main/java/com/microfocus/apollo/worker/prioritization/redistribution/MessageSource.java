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

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.microfocus.apollo.worker.prioritization.management.Queue;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConfirmListener;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.ShutdownSignalException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

public class MessageSource {
    private static final Logger LOGGER = LoggerFactory.getLogger(MessageSource.class);
    private final ConnectionFactory connectionFactory;
    private Channel incomingChannel;
    private Channel outgoingChannel;
    private final MessageTarget messageTarget;
    private final Queue sourceQueue;
    private ShutdownSignalException shutdownSignalException = null;
    private final ConcurrentNavigableMap<Long, Long> outstandingConfirms = new ConcurrentSkipListMap<>();

    private boolean active;

    public boolean isCancelled() {
        return cancelled;
    }

    private boolean cancelled;
    
    private final Gson gson = new Gson();

    public MessageSource(final ConnectionFactory connectionFactory, final Queue sourceQueue, final MessageTarget messageTarget) {
        this.connectionFactory = connectionFactory;
        this.messageTarget = messageTarget;
        this.sourceQueue = sourceQueue;
    }
    
    public void init() throws IOException, TimeoutException {
        final Connection connection = connectionFactory.newConnection();
        this.incomingChannel = connection.createChannel();
//        this.incomingChannel.basicQos(100);
        this.outgoingChannel = connection.createChannel();
        registerConfirmListener();
    }
    
    private void registerConfirmListener() throws IOException {
        outgoingChannel.addConfirmListener(new ConfirmListener() {
            @Override
            public void handleAck(long deliveryTag, boolean multiple) throws IOException {
                if (multiple) {
                    final ConcurrentNavigableMap<Long, Long> confirmed = outstandingConfirms.headMap(
                            deliveryTag, true
                    );
                    
                    LOGGER.info("Ack message source delivery {} from {} after publish confirm {} of message to {}",
                            confirmed.lastKey(), sourceQueue.getName(), outstandingConfirms.get(deliveryTag), 
                            messageTarget.getTargetQueueName());

                    incomingChannel.basicAck(confirmed.lastKey(), true);
                    confirmed.clear();
                } else {
                    LOGGER.info("Ack message source delivery {} from {} after publish confirm {} of message to {}",
                            outstandingConfirms.get(deliveryTag), sourceQueue.getName(), outstandingConfirms.get(deliveryTag),
                            messageTarget.getTargetQueueName());
                    
                    incomingChannel.basicAck(outstandingConfirms.get(deliveryTag), false);
                    outstandingConfirms.remove(deliveryTag);
                }
            }

            @Override
            public void handleNack(long deliveryTag, boolean multiple) throws IOException {
                if (multiple) {
                    final ConcurrentNavigableMap<Long, Long> confirmed = outstandingConfirms.headMap(
                            deliveryTag, true
                    );

                    for(final Long messageDeliveryTagToNack: confirmed.values()) {
                        try {
                            incomingChannel.basicNack(messageDeliveryTagToNack, true, true);
                        }
                        catch (final IOException e) {
                            //TODO Consider allowing a retry limit before escalating and removing this messageSource
                            LOGGER.error("Exception ack'ing '{}' {}",
                                    messageDeliveryTagToNack,
                                    e.toString());
                        }
                    }

                    confirmed.clear();
                } else {
                    incomingChannel.basicNack(outstandingConfirms.get(deliveryTag), false, true);
                    outstandingConfirms.remove(deliveryTag);
                }
            }
        });
        outgoingChannel.confirmSelect();        
    }

    public void run(final long consumptionLimit) {

        if(active) {
            LOGGER.info("Source queue '{}' is still active, ignoring wireup request.", sourceQueue.getName());
            return;
        }

        try {
            final AtomicInteger messageCount = new AtomicInteger(0);
            incomingChannel.basicConsume(sourceQueue.getName(),
                    (consumerTag, message) -> {

                        final Envelope envelope = message.getEnvelope();
                        final long nextPublishSequenceNumber = outgoingChannel.getNextPublishSeqNo();
                        try {
                            LOGGER.info("Publishing message source {} from {} to {} and expecting publish confirm {}",
                                    envelope.getDeliveryTag(), sourceQueue.getName(), 
                                    messageTarget.getTargetQueueName(), nextPublishSequenceNumber);

                            outstandingConfirms.put(nextPublishSequenceNumber, envelope.getDeliveryTag());
                            AMQP.BasicProperties basicProperties = new AMQP.BasicProperties.Builder()
                                    .contentType(message.getProperties().getContentType())
                                    .deliveryMode(message.getProperties().getDeliveryMode())
                                    .priority(message.getProperties().getPriority())
                                    .build();
                            
                            //Hack the To
                            final JsonObject jsonObject = 
                                    gson.fromJson(new String(message.getBody(), StandardCharsets.UTF_8), JsonObject.class);
                            jsonObject.addProperty("to", messageTarget.getTargetQueueName());
                            final String s = gson.toJson(jsonObject);
                            
                            outgoingChannel.basicPublish("",
                                    messageTarget.getTargetQueueName(), basicProperties, s.getBytes(StandardCharsets.UTF_8));
                        }
                        catch (final IOException e) {
                            LOGGER.error("Exception publishing to '{}' {}", messageTarget.getTargetQueueName(),
                                    e.toString());
                            //TODO Consider allowing a retry limit before escalating and stopping this MessageTarget
                            outstandingConfirms.remove(nextPublishSequenceNumber);
                            incomingChannel.basicCancel(consumerTag);
                            
                        }
                        
                        messageCount.incrementAndGet();

                        if(messageCount.get() >= consumptionLimit) {
                            LOGGER.info("Consumption target '{}' reached for '{}'.", consumptionLimit,
                                    sourceQueue.getName());
                            if (!cancelled) {
                                incomingChannel.basicCancel(consumerTag);
                                cancelled = true;
                            }
                        }
                    },
                    consumerTag -> {
                        //Stop tracking that we are consuming from the consumerTag queue
                        active = false;
                    },
                    (consumerTag, sig) -> {
                        //Connection lost, give up
                        shutdownSignalException = sig;
                        active = false;
                    });
            active = true;
        } catch (final IOException e) {
            LOGGER.error("Exception registering MessageSource from '{}' queue", sourceQueue.getName());
            active = false;
        }

    }

    public ShutdownSignalException getShutdownSignalException() {
        return shutdownSignalException;
    }
    
    public Queue getSourceQueue() {
        return sourceQueue;
    }
}
