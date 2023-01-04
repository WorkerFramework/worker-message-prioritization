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
package com.microfocus.apollo.worker.prioritization.redistribution.lowlevel;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.microfocus.apollo.worker.prioritization.rabbitmq.Queue;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Envelope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;

public class StagingQueueTargetQueuePair {
    private static final Logger LOGGER = LoggerFactory.getLogger(StagingQueueTargetQueuePair.class);

    private final Connection connection;
    private final Queue stagingQueue;
    private final Queue targetQueue;
    private Channel stagingQueueChannel;
    private Channel targetQueueChannel;
    private final ConcurrentNavigableMap<Long, Long> outstandingConfirms = new ConcurrentSkipListMap<>();
    private final Gson gson = new Gson();
    private final AtomicInteger messageCount = new AtomicInteger(0);
    private final long consumptionLimit;
    private StagingQueueConsumer stagingQueueConsumer;

    public StagingQueueTargetQueuePair(final Connection connection, 
                                       final Queue stagingQueue, final Queue targetQueue,
                                       final long consumptionLimit) {
        this.connection = connection;
        this.stagingQueue = stagingQueue;
        this.targetQueue = targetQueue;
        this.consumptionLimit = consumptionLimit;
    }
    
    public void start() throws IOException {
        stagingQueueChannel = connection.createChannel();
        targetQueueChannel = connection.createChannel();

        final var targetQueueConfirmListener = new TargetQueueConfirmListener(this);
        targetQueueChannel.addConfirmListener(targetQueueConfirmListener);

        stagingQueueConsumer = new StagingQueueConsumer(stagingQueueChannel, this);

        stagingQueueChannel.basicConsume(stagingQueue.getName(), stagingQueueConsumer);
    }

    public void publishToTarget(final String consumerTag, final Envelope envelope, 
                                final AMQP.BasicProperties properties, final byte[] body)
            throws IOException {

        long nextPublishSeqNo = targetQueueChannel.getNextPublishSeqNo();
        targetQueueChannel.basicPublish("", targetQueue.getName(), properties, body);
        outstandingConfirms.put(nextPublishSeqNo, envelope.getDeliveryTag());

        try {
            LOGGER.info("Publishing message source {} from {} to {} and expecting publish confirm {}",
                    envelope.getDeliveryTag(), stagingQueue.getName(),
                    targetQueue.getName(), nextPublishSeqNo);

            AMQP.BasicProperties basicProperties = new AMQP.BasicProperties.Builder()
                    .contentType(properties.getContentType())
                    .deliveryMode(properties.getDeliveryMode())
                    .priority(properties.getPriority())
                    .build();

            //Hack the To
            final JsonObject jsonObject =
                    gson.fromJson(new String(body, StandardCharsets.UTF_8), JsonObject.class);
            jsonObject.addProperty("to", targetQueue.getName());
            final String s = gson.toJson(jsonObject);

            targetQueueChannel.basicPublish("", targetQueue.getName(), basicProperties, 
                    s.getBytes(StandardCharsets.UTF_8));
        }
        catch (final IOException e) {
            LOGGER.error("Exception publishing to '{}' {}", targetQueue.getName(),
                    e.toString());
            stagingQueueChannel.basicCancel(consumerTag);

        }

        messageCount.incrementAndGet();

        if(messageCount.get() >= consumptionLimit) {
            LOGGER.info("Consumption target '{}' reached for '{}'.", consumptionLimit,
                    stagingQueue.getName());
            if (stagingQueueConsumer.isActive()) {
                stagingQueueChannel.basicCancel(consumerTag);
                stagingQueueConsumer.setActive(false);
            }
        }       
        
        
        
    }
    
    public void handleDeliveryToTargetQueueAck(final long deliveryTag, final boolean multiple) throws IOException {
        if (multiple) {
            final var confirmed = outstandingConfirms.headMap(
                    deliveryTag, true
            );

            LOGGER.trace("Ack message source delivery {} from {} after publish confirm {} of message to {}",
                    confirmed.lastKey(), stagingQueue.getName(), outstandingConfirms.get(deliveryTag),
                    targetQueue.getName());

            stagingQueueChannel.basicAck(confirmed.lastKey(), true);
            confirmed.clear();
        } else {
            LOGGER.trace("Ack message source delivery {} from {} after publish confirm {} of message to {}",
                    outstandingConfirms.get(deliveryTag), stagingQueue.getName(), outstandingConfirms.get(deliveryTag),
                    targetQueue.getName());

            stagingQueueChannel.basicAck(outstandingConfirms.get(deliveryTag), false);
            outstandingConfirms.remove(deliveryTag);
        }
    }
    
    public void handleDeliveryToTargetQueueNack(final long deliveryTag, final boolean multiple) throws IOException {
        if (multiple) {
            final var confirmed = outstandingConfirms.headMap(
                    deliveryTag, true
            );

            for(final Long messageDeliveryTagToNack: confirmed.values()) {
                try {
                    stagingQueueChannel.basicNack(messageDeliveryTagToNack, true, true);
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
            stagingQueueChannel.basicNack(outstandingConfirms.get(deliveryTag), false, true);
            outstandingConfirms.remove(deliveryTag);
        }        
    }
}
