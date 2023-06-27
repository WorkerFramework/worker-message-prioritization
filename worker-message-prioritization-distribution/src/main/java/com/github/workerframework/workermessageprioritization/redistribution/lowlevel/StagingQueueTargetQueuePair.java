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
import com.rabbitmq.client.AlreadyClosedException;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.ShutdownSignalException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;
import java.util.Optional;
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
    private long consumerPublisherPairRunningTooLongTimeoutMilliseconds;
    private Instant startTime;

    public StagingQueueTargetQueuePair(final Connection connection,
                                       final Queue stagingQueue,
                                       final Queue targetQueue,
                                       final long consumptionLimit,
                                       final long consumerPublisherPairRunningTooLongTimeoutMilliseconds) {
        this.connection = connection;
        this.stagingQueue = stagingQueue;
        this.targetQueue = targetQueue;
        this.consumptionLimit = consumptionLimit;
        this.consumerPublisherPairRunningTooLongTimeoutMilliseconds = consumerPublisherPairRunningTooLongTimeoutMilliseconds;
    }
    
    public void startConsuming() throws IOException {
        startTime = Instant.now();
        stagingQueueChannel = connection.createChannel();
        // The cast to int below is fine as I don't think consumptionLimit will be greater than Integer.MAX_VALUE. If it is, it doesn't
        // really matter, as we'll consume the remaining messages during the next run(s) of the LowLevelDistributor.
        stagingQueueChannel.basicQos((int)consumptionLimit);
        targetQueueChannel = connection.createChannel();
        targetQueueChannel.confirmSelect();

        final TargetQueueConfirmListener targetQueueConfirmListener = new TargetQueueConfirmListener(this);
        targetQueueChannel.addConfirmListener(targetQueueConfirmListener);

        stagingQueueConsumer = new StagingQueueConsumer(stagingQueueChannel, this);

        stagingQueueChannel.basicConsume(stagingQueue.getName(), stagingQueueConsumer);
    }

    public void handleStagedMessage(final String consumerTag, final Envelope envelope,
                                    final AMQP.BasicProperties properties, final byte[] body)
            throws IOException {

        // When a consumer has consumed the number of messages specified by consumptionLimit, it will be cancelled.
        //
        // However, calling basicCancel can take some time to take effect.
        //
        // From: https://github.com/rabbitmq/rabbitmq-dotnet-client/issues/340#issuecomment-319649377
        //
        // "Cancelling a consumer cannot cancel messages that are "in flight". Closing a channel can't either because basic.cancel
        // takes time to reach the server and be processed. Use prefetch to limit the number of outstanding deliveries. A flag
        // in your code that tells the consumer ignore or requeue (if you close the channel, explicitly requeueing isn't
        // necessary) deliveries that arrive after."
        //
        // As such, after a period of time after it has been cancelled, a consumer may still consume messages from the staging queue
        // over and above consumptionLimit.
        //
        // If we detect this scenario, we need to put the message back on the staging queue so another consumer can pick it up.

        if (stagingQueueConsumer.isCancellationRequested() || stagingQueueConsumer.isCancelled()) {

            final long deliveryTag = envelope.getDeliveryTag();

            LOGGER.debug("handleStagedMessage called with consumerTag {} and deliveryTag {}," +
                            "but stagingQueueConsumer has been cancelled (or cancellation has been requested and is pending), " +
                            "so calling stagingQueueChannel.basicReject({}, true) to put this message back on the {} staging queue. " +
                            "The StagingQueueTargetQueuePair this message relates to is {}",
                    consumerTag,
                    deliveryTag,
                    deliveryTag,
                    stagingQueue.getName(),
                    this);

            try {
                stagingQueueChannel.basicReject(deliveryTag, true);

                LOGGER.debug("Successfully called stagingQueueChannel.basicReject({}, true) " +
                                "to put this message back on the {} staging queue. " +
                                "The StagingQueueTargetQueuePair this message relates to is {}",
                        deliveryTag,
                        stagingQueue.getName(),
                        this);
            } catch (final IOException basicRejectException) {
                final String errorMessage = String.format(
                        "Exception calling stagingQueueChannel.basicReject(%s, true) to put this message back on the %s staging queue. " +
                                "The StagingQueueTargetQueuePair this message relates to is %s",
                        deliveryTag, stagingQueue.getName(), this);

                LOGGER.warn(errorMessage, basicRejectException);
            } finally {
                return;
            }
        } else if (messageCount.get() >= consumptionLimit) {

            final long deliveryTag = envelope.getDeliveryTag();

            LOGGER.debug("Consumption target '{}' reached for '{}'. Number of messages consumed by this consumer was '{}'. " +
                            "Calling stagingQueueChannel.basicReject({}, true) to put this message back on the staging queue. " +
                            "Calling stagingQueueChannel.basicCancel({}) to cancel this consumer. " +
                            "The StagingQueueTargetQueuePair this message relates to is '{}'",
                    consumptionLimit,
                    stagingQueue.getName(),
                    messageCount.get(),
                    deliveryTag,
                    consumerTag,
                    this);

            try {
                stagingQueueChannel.basicReject(deliveryTag, true);

                LOGGER.debug("Successfully called stagingQueueChannel.basicReject({}, true) " +
                                "to put this message back on the {} staging queue. " +
                                "The StagingQueueTargetQueuePair this message relates to is {}",
                        deliveryTag,
                        stagingQueue.getName(),
                        this);
            } catch (final IOException basicRejectException) {
                final String errorMessage = String.format(
                        "Exception calling basicReject(%s, true) to put this message back on the %s staging queue. " +
                                "The StagingQueueTargetQueuePair this message relates to is %s",
                        deliveryTag, stagingQueue.getName(), this);

                LOGGER.warn(errorMessage, basicRejectException);
            }

            try {
                stagingQueueConsumer.recordCancellationRequested();

                stagingQueueChannel.basicCancel(consumerTag);

                LOGGER.debug("Successfully called stagingQueueChannel.basicCancel({}) to cancel this consumer. " +
                                "The StagingQueueTargetQueuePair this message relates to is {}", consumerTag, this);
            } catch (final IOException basicCancelException) {
                final String errorMessage = String.format(
                        "Exception calling stagingQueueChannel.basicCancel(%s). " +
                                "The StagingQueueTargetQueuePair this message relates to is '%s'",
                        consumerTag, this);

                LOGGER.error(errorMessage, basicCancelException);
            } finally  {
                return;
            }
        }

        long nextPublishSeqNo = targetQueueChannel.getNextPublishSeqNo();
        outstandingConfirms.put(nextPublishSeqNo, envelope.getDeliveryTag());

        try {
            LOGGER.debug("Publishing message source {} from {} to {} and expecting publish confirm {}",
                    envelope.getDeliveryTag(), stagingQueue.getName(),
                    targetQueue.getName(), nextPublishSeqNo);

            AMQP.BasicProperties basicProperties = new AMQP.BasicProperties.Builder()
                    .contentType(properties.getContentType())
                    .deliveryMode(properties.getDeliveryMode())
                    .priority(properties.getPriority())
                    .build();

            targetQueueChannel.basicPublish("", targetQueue.getName(), basicProperties, body);
        }
        catch (final IOException basicPublishException) {

            LOGGER.error("Exception publishing to '{}' for StagingQueueTargetQueuePair '{}' {}",
                    targetQueue.getName(), this, basicPublishException.toString());

            try {
                stagingQueueConsumer.recordCancellationRequested();

                stagingQueueChannel.basicCancel(consumerTag);

                LOGGER.debug("Successfully called stagingQueueChannel.basicCancel({}) to cancel this consumer. " +
                        "The StagingQueueTargetQueuePair this message relates to is {}", consumerTag, this);
            } catch (final IOException basicCancelException) {
                final String errorMessage = String.format(
                        "Exception calling stagingQueueChannel.basicCancel(%s). " +
                                "The StagingQueueTargetQueuePair this message relates to is '%s'",
                        consumerTag, this);

                LOGGER.error(errorMessage, basicCancelException);
            }
        }

        messageCount.incrementAndGet();
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

    public boolean isStagingQueueChannelOpen() {
        return stagingQueueChannel != null ? stagingQueueChannel.isOpen() : true;
    }

    public boolean isTargetQueueChannelOpen() {
        return targetQueueChannel != null ? targetQueueChannel.isOpen() : true;
    }

    private boolean isConsumerCancellationRequested() {
        return stagingQueueConsumer != null ? stagingQueueConsumer.isCancellationRequested() : false;
    }

    private Optional<Instant> getConsumerCancellationRequestSentTime()
    {
        return stagingQueueConsumer.getCancellationRequestSentTime();
    }

    public boolean isConsumerCompleted() {
        return stagingQueueConsumer != null ? stagingQueueConsumer.isCancelled() : false;
    }

    public boolean isPublisherCompleted() {
        return outstandingConfirms.isEmpty();
    }

    public boolean isRunningTooLong() {
        if (consumerPublisherPairRunningTooLongTimeoutMilliseconds == 0) {
            // A value of 0 means this feature has been disabled
            return false;
        } else {
            return Instant.now().toEpochMilli() > (startTime.toEpochMilli() + consumerPublisherPairRunningTooLongTimeoutMilliseconds);
        }
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
        final String shutdownSignalExceptionMessage =
                stagingQueueConsumer != null && stagingQueueConsumer.getShutdownSignalException() != null
                ? stagingQueueConsumer.getShutdownSignalException().toString()
                : null;

        return MoreObjects.toStringHelper(this)
                .add("identifier", getIdentifier())
                .add("startTime", startTime)
                .add("consumerPublisherPairRunningTooLongTimeoutMilliseconds", consumerPublisherPairRunningTooLongTimeoutMilliseconds)
                .add("consumptionLimit", consumptionLimit)
                .add("messageCount", messageCount.get())
                .add("consumerCancellationRequested", isConsumerCancellationRequested())
                .add("consumerCancellationRequestSentTime", getConsumerCancellationRequestSentTime())
                .add("consumerCompleted", isConsumerCompleted())
                .add("numOutstandingConfirms", outstandingConfirms.keySet().size())
                .add("publisherCompleted", isPublisherCompleted())
                .add("shutdownSignalException", shutdownSignalExceptionMessage)
                .add("stagingQueueChannelOpen", isStagingQueueChannelOpen())
                .add("targetQueueChannelOpen", isTargetQueueChannelOpen())
                .toString();
    }

    public void close()
    {
        try {
            stagingQueueChannel.close();
        } catch (final AlreadyClosedException e) {
            // Ignore
        } catch (final Exception e) {
            LOGGER.warn("Exception closing stagingQueueChannel", e);
        }

        try {
            targetQueueChannel.close();
        } catch (final AlreadyClosedException e) {
            // Ignore
        } catch (final Exception e) {
            LOGGER.warn("Exception closing stagingQueueChannel", e);
        }

        // Don't close the connection, as it is shared with other StagingQueueTargetQueuePairs
    }
}
