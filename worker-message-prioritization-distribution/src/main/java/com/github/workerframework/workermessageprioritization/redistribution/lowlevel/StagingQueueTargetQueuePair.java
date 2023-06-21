/*
 * Copyright 2022-2023 Open Text.
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
package com.github.workerframework.workermessageprioritization.redistribution.lowlevel;

import com.github.workerframework.workermessageprioritization.rabbitmq.Queue;
import com.google.common.base.MoreObjects;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.ShutdownSignalException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
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
    
    public void startConsuming() throws IOException {
        stagingQueueChannel = connection.createChannel();
        stagingQueueChannel.addShutdownListener(cause -> {
            if (cause.isInitiatedByApplication()) {
                LOGGER.debug("Shutdown of staging queue channel for {} initiated by application", stagingQueue.getName());
            }
            else {
                LOGGER.debug(String.format(
                        "Shutdown of staging queue channel for %s not initiated by application", stagingQueue.getName()), cause);
            }
        });
        // The cast to int below is fine as I don't think consumptionLimit will be greater than Integer.MAX_VALUE. If it is, it doesn't
        // really matter, as we'll consume the remaining messages during the next run(s) of the LowLevelDistributor.
        stagingQueueChannel.basicQos((int)consumptionLimit);
        targetQueueChannel = connection.createChannel();
        targetQueueChannel.addShutdownListener(cause -> {
            if (cause.isInitiatedByApplication()) {
                LOGGER.debug("Shutdown of target queue channel for {} initiated by application", targetQueue.getName());
            }
            else {
                LOGGER.debug(String.format(
                        "Shutdown of target queue channel for %s not initiated by application", targetQueue.getName()), cause);
            }
        });
        targetQueueChannel.confirmSelect();

        final TargetQueueConfirmListener targetQueueConfirmListener = new TargetQueueConfirmListener(this);
        targetQueueChannel.addConfirmListener(targetQueueConfirmListener);

        stagingQueueConsumer = new StagingQueueConsumer(stagingQueueChannel, this);

        stagingQueueChannel.basicConsume(stagingQueue.getName(), stagingQueueConsumer);
    }

    public void handleStagedMessage(final String consumerTag, final Envelope envelope,
                                    final AMQP.BasicProperties properties, final byte[] body)
            throws IOException {

        long nextPublishSeqNo = targetQueueChannel.getNextPublishSeqNo();
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

            targetQueueChannel.basicPublish("", targetQueue.getName(), basicProperties, body);
        }
        catch (final IOException e) {
            LOGGER.error("Exception publishing to '{}' {}", targetQueue.getName(),
                    e.toString());
            stagingQueueChannel.basicCancel(consumerTag);

        }

        messageCount.incrementAndGet();

        if(messageCount.get() >= consumptionLimit) {
            LOGGER.info("Consumption target '{}' reached for '{}'. Number of messages consumed: '{}'", consumptionLimit,
                    stagingQueue.getName(), messageCount.get());
            if (!stagingQueueConsumer.isCancelled()) {
                // From: https://github.com/rabbitmq/rabbitmq-dotnet-client/issues/340#issuecomment-319649377
                //
                // "Cancelling a consumer cannot cancel messages that are "in flight". Closing a channel can't either because
                // basicCancel takes time to reach the server and be processed. Use prefetch to limit the number of outstanding
                // deliveries."
                //
                // i.e. Calling basicCancel won't prevent the consumer from consuming > consumptionLimit. To do that, we are setting
                // the prefetch count (basicQos) to consumptionLimit in this class's constructor.
                stagingQueueChannel.basicCancel(consumerTag);
            }
        }
        
    }
    
    public void handleDeliveryToTargetQueueAck(final long deliveryTag, final boolean multiple) throws IOException {
        if (multiple) {
            final ConcurrentNavigableMap<Long, Long> confirmed = outstandingConfirms.headMap(
                    deliveryTag, true
            );

            LOGGER.trace("Ack (multiple) message source delivery {} from {} after publish confirm {} of message to {}",
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
        LOGGER.warn("Nack confirmation received for message(s) from '{}' published to '{}'",
                stagingQueue.getName(),
                targetQueue.getName());
        
        if (multiple) {
            final ConcurrentNavigableMap<Long, Long> confirmed = outstandingConfirms.headMap(
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

    public String getIdentifier() {
        return stagingQueue.getName() + ">" + targetQueue.getName();
    }
    
    public boolean isCompleted() {
        return stagingQueueConsumer.isCancelled();
    }
    
    public ShutdownSignalException getShutdownSignalException() {
        return stagingQueueConsumer.getShutdownSignalException();
    }

    public long getConsumptionLimit() {
        return consumptionLimit;
    }

    @Override
    public String toString()
    {
        return MoreObjects.toStringHelper(this)
                .add("getIdentifier()", getIdentifier())
                .add("consumptionLimit", consumptionLimit)
                .add("messageCount", messageCount.get())
                .add("numOutstandingConfirms", outstandingConfirms.keySet().size())
                .add("isCompleted()", stagingQueueConsumer != null ? isCompleted() : false)
                .add("getShutdownSignalException()", stagingQueueConsumer != null ? getShutdownSignalException() : null)
                .toString();
    }
}
