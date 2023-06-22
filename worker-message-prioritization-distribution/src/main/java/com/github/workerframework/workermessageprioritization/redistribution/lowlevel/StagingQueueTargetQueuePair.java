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
import com.google.common.collect.Lists;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.ShutdownSignalException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;

import dev.failsafe.Failsafe;
import dev.failsafe.Fallback;
import dev.failsafe.Policy;
import dev.failsafe.RetryPolicy;

public class StagingQueueTargetQueuePair {
    private static final Logger LOGGER = LoggerFactory.getLogger(StagingQueueTargetQueuePair.class);

    // A retry policy for when we get an exception when using the RabbitMQ client.
    private static final RetryPolicy<Void> RABBIT_RETRY_POLICY =  RetryPolicy.<Void>builder()
            .handle(IOException.class)
            .withDelay(Duration.ofSeconds(1))
            .onRetry(e -> LOGGER.warn("Failure #{}. Retrying.", e.getAttemptCount()))
            .withMaxAttempts(3)
            .build();

    // Fallback.none() prevents the exception from being rethrown after all retries have failed.
    // See: https://github.com/failsafe-lib/failsafe/issues/253#issuecomment-642142147
    private static final Fallback<Void> RABBIT_RETRY_POLICY_FALLBACK = Fallback.none();

    // Make sure that the fallback policy is before the retry policy.
    private static final List<Policy<Void>>
            RABBIT_RETRY_POLICIES = Lists.newArrayList(RABBIT_RETRY_POLICY_FALLBACK, RABBIT_RETRY_POLICY);

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
        if (stagingQueueConsumer.isCancelled()) {

            final long deliveryTag = envelope.getDeliveryTag();

            LOGGER.info("handleStagedMessage called with consumerTag {} and deliveryTag {}," +
                            "but stagingQueueConsumer.isCancelled() = true, " +
                            "so calling stagingQueueChannel.basicReject({}, true) to put this message back on the {} staging queue. " +
                            "The StagingQueueTargetQueuePair this message relates to is {}",
                    consumerTag,
                    deliveryTag,
                    deliveryTag,
                    stagingQueue.getName(),
                    this);

            try {
                Failsafe.with(RABBIT_RETRY_POLICIES)
                        .onFailure(failure -> {
                            final String errorMessage = String.format(
                                    "Failed to call stagingQueueChannel.basicReject(%s, true) (attempted %s times) to try and put this " +
                                            "message back on the %s staging queue. The StagingQueueTargetQueuePair this message " +
                                            "relates to is %s",
                                    deliveryTag, failure.getAttemptCount(), stagingQueue.getName(), this);

                            LOGGER.error(errorMessage, failure.getException());

                            // TODO We've tried 3 times to call basicReject to put message back on staging queue, but failed. What
                            //  should we do now?
                        })
                        .run(() -> {
                            stagingQueueChannel.basicReject(deliveryTag, true);

                            LOGGER.debug("Successfully called stagingQueueChannel.basicReject({}, true) " +
                                            "to put this message back on the {} staging queue. " +
                                            "The StagingQueueTargetQueuePair this message relates to is {}",
                                    deliveryTag,
                                    stagingQueue.getName(),
                                    this);
                        });
            } finally {
                return;
            }
        }

        // If the consumer has not been cancelled, but has consumed the number of messages specified by consumptionLimit, we need
        // to put the message back on the staging queue so another consumer can pick it up. We also need to cancel the consumer to stop
        // it consuming any more messages from the staging queue.
        if (!stagingQueueConsumer.isCancelled() && messageCount.get() >= consumptionLimit) {

            final long deliveryTag = envelope.getDeliveryTag();

            LOGGER.info("Consumption target '{}' reached for '{}'. Number of messages consumed by this consumer was '{}'. " +
                            "Calling stagingQueueChannel.basicReject({}, true) to put this message back on the staging queue. " +
                            "Calling stagingQueueChannel.basicCancel({}) to cancel this consumer. " +
                            "The StagingQueueTargetQueuePair this message relates to is '{}'",
                    consumptionLimit,
                    stagingQueue.getName(),
                    messageCount.get(),
                    deliveryTag,
                    consumerTag,
                    this);

            Failsafe.with(RABBIT_RETRY_POLICIES)
                    .onFailure(failure -> {
                        final String errorMessage = String.format(
                                "Failed to call stagingQueueChannel.basicReject(%s, true) (attempted %s times) to try and put this " +
                                        "message back on the %s staging queue. The StagingQueueTargetQueuePair this message relates to " +
                                        "is %s",
                                deliveryTag, failure.getAttemptCount(), stagingQueue.getName(), this);

                        LOGGER.error(errorMessage, failure.getException());

                        // TODO We've tried 3 times to call basicReject to put message back on staging queue, but failed. What
                        //  should we do now? (remember we're about to cancel the consumer now as well below)
                    })
                    .run(() -> {
                       stagingQueueChannel.basicReject(deliveryTag, true);

                        LOGGER.debug("Successfully called stagingQueueChannel.basicReject({}, true) " +
                                        "to put this message back on the {} staging queue. " +
                                        "The StagingQueueTargetQueuePair this message relates to is {}",
                                deliveryTag,
                                stagingQueue.getName(),
                                this);
                    });

            try {
                stagingQueueChannel.basicCancel(consumerTag);

                LOGGER.debug("Successfully called stagingQueueChannel.basicCancel({}) to cancel this consumer. " +
                                "The StagingQueueTargetQueuePair this message relates to is {}", consumerTag, this);
            } catch (final IOException basicCancelException) {
                if (basicCancelException.getMessage().contains("Unknown consumerTag")) {
                    final String errorMessage = String.format(
                            "Ignoring exception calling stagingQueueChannel.basicCancel(%s), " +
                                    "as it looks like the consumer has already been cancelled (cancelling a consumer can take some time, " +
                                    "so this scenario is to be expected). The StagingQueueTargetQueuePair this message relates to is %s",
                            consumerTag, this);

                    LOGGER.debug(errorMessage, basicCancelException);
                } else {
                    final String errorMessage = String.format(
                            "Exception calling stagingQueueChannel.basicCancel(%s). " +
                                    "The StagingQueueTargetQueuePair this message relates to is '%s'",
                            consumerTag, this);

                    LOGGER.error(errorMessage, basicCancelException);
                }
            } finally  {
                return;
            }
        }

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
        catch (final IOException basicPublish) {
            // TODO Should we call basicReject to put message back on stagingQueue if basicPublish fails?
            // TODO What if basicReject also fails?
            LOGGER.error("Exception publishing to '{}' {}", targetQueue.getName(),
                basicPublish.toString());

            // TODO handle basicCancel exception
            stagingQueueChannel.basicCancel(consumerTag);
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

            // TODO what if basicAck fails? I think message is redelivered to stagingQueue so maybe this is ok?
            stagingQueueChannel.basicAck(confirmed.lastKey(), true);
            confirmed.clear();
        } else {
            LOGGER.trace("Ack message source delivery {} from {} after publish confirm {} of message to {}",
                    outstandingConfirms.get(deliveryTag), stagingQueue.getName(), outstandingConfirms.get(deliveryTag),
                    targetQueue.getName());

            // TODO what if basicAck fails? I think message is redelivered to stagingQueue so maybe this is ok?
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
            // TODO what if basicNack fails?
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
        final String shutdownSignalExceptionMessage =
                stagingQueueConsumer != null && stagingQueueConsumer.getShutdownSignalException() != null
                ? stagingQueueConsumer.getShutdownSignalException().toString()
                : null;

        return MoreObjects.toStringHelper(this)
                .add("getIdentifier()", getIdentifier())
                .add("consumptionLimit", consumptionLimit)
                .add("messageCount", messageCount.get())
                .add("numOutstandingConfirms", outstandingConfirms.keySet().size())
                .add("isCompleted()", stagingQueueConsumer != null ? isCompleted() : false)
                .add("getShutdownSignalException()", shutdownSignalExceptionMessage)
                .toString();
    }
}
