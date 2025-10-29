/*
 * Copyright 2022-2025 Open Text.
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
    private final long consumerPublisherPairLastDoneWorkTimeoutMilliseconds;
    private Instant startTime;
    private Instant timeSinceLastDoneWork;

    public StagingQueueTargetQueuePair(final Connection connection,
                                       final Queue stagingQueue,
                                       final Queue targetQueue,
                                       final long consumptionLimit,
                                       final long consumerPublisherPairLastDoneWorkTimeoutMilliseconds) {
        this.connection = connection;
        this.stagingQueue = stagingQueue;
        this.targetQueue = targetQueue;
        this.consumptionLimit = consumptionLimit;
        this.consumerPublisherPairLastDoneWorkTimeoutMilliseconds = consumerPublisherPairLastDoneWorkTimeoutMilliseconds;
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
        //
        // If the consumer is cancelled for reasons *other* than the consumption limit being reached, then the
        // StagingQueueConsumer.handleCancel (as opposed to StagingQueueConsumer.handleCancelOk) callback will be invoked. If that
        // happens, we don't really know what state the system will be in, so we will also close the staging queue channel in that
        // scenario, to ensure that unacked messages (unconfirmed deliveries) are requeued to the staging queue as quickly as possible.
        //
        // If we didn't close the staging queue channel, we'd potentially have to wait for the
        // https://www.rabbitmq.com/consumers.html#acknowledgement-timeout to expire before the unacked messages are requeued.
        //
        // Its important that the staging queue channel is NOT closed if the consumer was cancelled due to the consumption limit
        // being reached, as we need the staging queue channel to stay open long enough to:
        //
        // 1) Requeue messages (stagingQueueChannel.basicReject(deliveryTag, true)) arriving after stagingQueueChannel.basicCancel
        // (consumerTag) has been called but before the StagingQueueConsumer.handleCancelOk callback is invoked
        //
        // 2) Send acks back to the staging queue for messages published to the target queue in handleDeliveryToTargetQueueAck

        if (stagingQueueConsumer.getCancellationRequestSentTime().isPresent() || stagingQueueConsumer.isCancelled()) {

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

            // If stagingQueueChannel.basicCancel encounters an error, StagingQueueConsumer.handleShutdownSignal will be called and the
            // stagingQueueChannel will be closed. Then, the next time the LowLevelDistributor runs, it will remove *this*
            // StagingQueueTargetQueuePair, allowing a new StagingQueueTargetQueuePair to be created (if still required).
            stagingQueueChannel.basicReject(deliveryTag, true);

            LOGGER.debug("Successfully called stagingQueueChannel.basicReject({}, true) " +
                            "to put this message back on the {} staging queue. " +
                            "The StagingQueueTargetQueuePair this message relates to is {}",
                    deliveryTag,
                    stagingQueue.getName(),
                    this);

            return;
        }

        long nextPublishSeqNo = targetQueueChannel.getNextPublishSeqNo();
        outstandingConfirms.put(nextPublishSeqNo, envelope.getDeliveryTag());

        LOGGER.debug("Publishing message source {} from {} to {} and expecting publish confirm {}",
                envelope.getDeliveryTag(), stagingQueue.getName(),
                targetQueue.getName(), nextPublishSeqNo);


        AMQP.BasicProperties basicProperties = new AMQP.BasicProperties.Builder()
                .headers(properties.getHeaders())
                .contentType(properties.getContentType())
                .deliveryMode(properties.getDeliveryMode())
                .build();

        // If targetQueueChannel.basicPublish encounters an error, the targetQueueChannel will be closed. Then, the next time the
        // LowLevelDistributor runs, it will remove *this* StagingQueueTargetQueuePair, allowing a new StagingQueueTargetQueuePair to
        // be created (if still required).
        targetQueueChannel.basicPublish("", targetQueue.getName(), basicProperties, body);

        messageCount.incrementAndGet();

        if (messageCount.get() >= consumptionLimit) {

            LOGGER.debug("Consumption target '{}' reached for '{}'. Number of messages consumed by this consumer was '{}'. " +
                            "Calling stagingQueueChannel.basicCancel({}) to cancel this consumer. " +
                            "The StagingQueueTargetQueuePair this message relates to is '{}'",
                    consumptionLimit,
                    stagingQueue.getName(),
                    messageCount.get(),
                    consumerTag,
                    this);

            cancelConsumer(consumerTag, StagingQueueConsumer.CancellationReason.CONSUMPTION_LIMIT_REACHED);
        }
    }

    private void cancelConsumer(final String consumerTag, final StagingQueueConsumer.CancellationReason cancellationReason)
            throws IOException {
        stagingQueueConsumer.recordCancellationRequested(cancellationReason);

        // If stagingQueueChannel.basicCancel encounters an error, StagingQueueConsumer.handleShutdownSignal will be called and the
        // stagingQueueChannel will be closed. Then, the next time the LowLevelDistributor runs, it will remove *this*
        // StagingQueueTargetQueuePair, allowing a new StagingQueueTargetQueuePair to be created (if still required).
        stagingQueueChannel.basicCancel(consumerTag);

        LOGGER.debug("Successfully called stagingQueueChannel.basicCancel({}) to cancel this consumer. Cancellation reason: {}. " +
                "The StagingQueueTargetQueuePair this message relates to is {}", consumerTag, cancellationReason, this);
    }

    public void handleDeliveryToTargetQueueAck(final long deliveryTag, final boolean multiple) throws IOException {
        // If stagingQueueChannel.basicAck encounters an error, we're relying on the IOException propagating up through:
        //
        // StagingQueueTargetQueuePair.handleDeliveryToTargetQueueAck
        // TargetQueueConfirmListener.handleAck
        // ChannelN.callConfirmListeners
        //
        // The DefaultExceptionHandler extends StrictExceptionHandler, which is an implementation of ExceptionHandler that DOES close
        // channels on unhandled consumer exception: https://www.rabbitmq.com/api-guide.html#unhandled-exceptions

        if (!stagingQueueChannel.isOpen()) {
            LOGGER.warn("handleDeliveryToTargetQueueAck called for deliveryTag {} and multiple {}, " +
                    "but the stagingQueueChannel is closed, so unable to send ack to stagingQueueChannel. " +
                    "The StagingQueueTargetQueuePair this message relates to is {}", deliveryTag, multiple, this);

            return;
        }

        if (multiple) {
            final ConcurrentNavigableMap<Long, Long> confirmed = outstandingConfirms.headMap(
                    deliveryTag, true
            );

            LOGGER.trace("Ack (multiple) message source delivery {} from {} after publish confirm {} of message to {}",
                    confirmed.lastKey(), stagingQueue.getName(), outstandingConfirms.get(deliveryTag),
                    targetQueue.getName());

            stagingQueueChannel.basicAck(confirmed.lastKey(), true);
            confirmed.clear();
            timeSinceLastDoneWork = Instant.now();
        } else {
            LOGGER.trace("Ack message source delivery {} from {} after publish confirm {} of message to {}",
                    outstandingConfirms.get(deliveryTag), stagingQueue.getName(), outstandingConfirms.get(deliveryTag),
                    targetQueue.getName());

            stagingQueueChannel.basicAck(outstandingConfirms.get(deliveryTag), false);
            outstandingConfirms.remove(deliveryTag);
            timeSinceLastDoneWork = Instant.now();
        }
    }

    public void handleDeliveryToTargetQueueNack(final long deliveryTag, final boolean multiple) throws IOException {
        // If stagingQueueChannel.basicNack encounters an error, we're relying on the IOException propagating up through
        //
        // StagingQueueTargetQueuePair.handleDeliveryToTargetQueueNack
        // TargetQueueConfirmListener.handleNack
        // ChannelN.callConfirmListeners
        //
        // The DefaultExceptionHandler extends StrictExceptionHandler, which is an implementation of ExceptionHandler that DOES close
        // channels on unhandled consumer exception: https://www.rabbitmq.com/api-guide.html#unhandled-exceptions

        if (!stagingQueueChannel.isOpen()) {
            LOGGER.warn("handleDeliveryToTargetQueueNack called for deliveryTag {} and multiple {}, " +
                    "but the stagingQueueChannel is closed, so unable to send nack to stagingQueueChannel. " +
                    "The StagingQueueTargetQueuePair this message relates to is {}", deliveryTag, multiple, this);

            return;
        }

        LOGGER.warn("Nack confirmation received for message(s) from '{}' published to '{}'",
                stagingQueue.getName(),
                targetQueue.getName());
        
        if (multiple) {
            final ConcurrentNavigableMap<Long, Long> confirmed = outstandingConfirms.headMap(
                    deliveryTag, true
            );

            for(final Long messageDeliveryTagToNack: confirmed.values()) {
                stagingQueueChannel.basicNack(messageDeliveryTagToNack, true, true);
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

    private Optional<Instant> getConsumerCancellationRequestSentTime()
    {
        return stagingQueueConsumer != null ? stagingQueueConsumer.getCancellationRequestSentTime() : Optional.empty();
    }

    private Optional<StagingQueueConsumer.CancellationReason> getConsumerCancellationReason()
    {
        return stagingQueueConsumer != null ? stagingQueueConsumer.getCancellationReason() : Optional.empty();
    }

    public boolean isConsumerCompleted() {
        return stagingQueueConsumer != null ? stagingQueueConsumer.isCancelled() : false;
    }

    public boolean isPublisherCompleted() {
        return outstandingConfirms.isEmpty();
    }

    public boolean hasExceededLastDoneWorkTimeout() {
        if (consumerPublisherPairLastDoneWorkTimeoutMilliseconds == 0) {
            // A value of 0 means this feature has been disabled
            return false;
        } else {
            if (timeSinceLastDoneWork != null) {
                return Instant.now().toEpochMilli() >
                        (timeSinceLastDoneWork.toEpochMilli() + consumerPublisherPairLastDoneWorkTimeoutMilliseconds);
            } else {
                // If timeSinceLastDoneWork is null, it means this StageQueueTargetQueuePair has not yet done any work yet, so in this
                // case we'll use the startTime instead to determine if the timeout has been exceeded.
                //
                // This could happen if the something goes wrong with the consumer before it has a chance to do any work, or possibly
                // if something is wrong with RabbitMQ. In any case, we'll want to ensure we return true here when the timeout has been
                // exceeded so that this StageQueueTargetQueuePair can be closed and removed, allowing a new instance to be created (if
                // required).
                return Instant.now().toEpochMilli() >
                        (startTime.toEpochMilli() + consumerPublisherPairLastDoneWorkTimeoutMilliseconds);
            }
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
                .add("timeSinceLastDoneWork", timeSinceLastDoneWork)
                .add("consumerPublisherPairLastDoneWorkTimeoutMilliseconds", consumerPublisherPairLastDoneWorkTimeoutMilliseconds)
                .add("consumptionLimit", consumptionLimit)
                .add("messageCount", messageCount.get())
                .add("consumerCancellationRequestSentTime", getConsumerCancellationRequestSentTime())
                .add("consumerCancellationReason", getConsumerCancellationReason())
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
