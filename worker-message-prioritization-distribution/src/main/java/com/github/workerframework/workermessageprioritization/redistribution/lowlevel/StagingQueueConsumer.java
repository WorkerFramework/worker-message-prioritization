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

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.AlreadyClosedException;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.ShutdownSignalException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;
import java.util.Optional;

public class StagingQueueConsumer extends DefaultConsumer {

    public enum CancellationReason {
        CONSUMPTION_LIMIT_REACHED,
        EXCEPTION_PUBLISHING_TO_TARGET_QUEUE
    };

    private static final Logger LOGGER = LoggerFactory.getLogger(StagingQueueConsumer.class);
    private final StagingQueueTargetQueuePair stagingQueueTargetQueuePair;
    private Optional<Instant> cancellationRequestSentTime = Optional.empty();
    private Optional<CancellationReason> cancellationReason = Optional.empty();
    private boolean cancelled = false;
    private ShutdownSignalException shutdownSignalException;

    public StagingQueueConsumer(final Channel channel, final StagingQueueTargetQueuePair stagingQueueTargetQueuePair) {
        super(channel);
        this.stagingQueueTargetQueuePair = stagingQueueTargetQueuePair;
    }

    public void recordCancellationRequested(final CancellationReason cancellationReason) {
        if (!this.cancellationRequestSentTime.isPresent()) {
            this.cancellationRequestSentTime = Optional.of(Instant.now());
        }

        if (!this.cancellationReason.isPresent()) {
            this.cancellationReason = Optional.of(cancellationReason);
        }
    }

    public Optional<CancellationReason> getCancellationReason() {
        return cancellationReason;
    }

    public Optional<Instant> getCancellationRequestSentTime() {
        return cancellationRequestSentTime;
    }

    public boolean isCancelled() {
        return cancelled;
    }
    
    public ShutdownSignalException getShutdownSignalException() {
        return shutdownSignalException;
    }
    
    @Override
    public void handleCancel(final String consumerTag) throws IOException {
        //Stop tracking that we are consuming from the consumerTag queue
        LOGGER.warn("handleCancel called for consumer with consumerTag {}", consumerTag);
        cancelled = true;

        // We won't have a cancellationReason here because handleCancel is called when the consumer is cancelled for reasons *other* than
        // by a call to Channel.basicCancel(java.lang.String). For example, the queue has been deleted.
        //
        // So, handleCancel being called probably indicates an error condition, as the consumer has been cancelled unexpectedly, so go
        // ahead and close the stagingQueueChannel to ensure any unacked messages are immediately requeued to the staging queue.
        closeStagingQueueChannel();
    }

    @Override
    public void handleCancelOk(final String consumerTag) {
        //Stop tracking that we are consuming from the consumerTag queue
        LOGGER.debug("handleCancelOk called for consumer with consumerTag {}", consumerTag);
        cancelled = true;

        if (cancellationReason.isPresent() && cancellationReason.get() == CancellationReason.EXCEPTION_PUBLISHING_TO_TARGET_QUEUE) {
            closeStagingQueueChannel();
        }
    }

    @Override
    public void handleShutdownSignal(final String consumerTag, final ShutdownSignalException sig) {
        //Connection lost, give up
        LOGGER.warn("handleShutdownSignal called for consumer with consumerTag {}", consumerTag);
        shutdownSignalException = sig;
        cancelled = true;
    }

    @Override
    public void handleDelivery(final String consumerTag, final Envelope envelope, 
                               final AMQP.BasicProperties properties, final byte[] body) throws IOException {


        LOGGER.debug("handleDelivery called for consumer with consumerTag {}", consumerTag);
        stagingQueueTargetQueuePair.handleStagedMessage(consumerTag, envelope, properties, body);

    }

    private void closeStagingQueueChannel() {
        try {
            getChannel().close();
        } catch (final AlreadyClosedException e) {
            // Ignore
        } catch (final Exception e) {
            LOGGER.warn("Exception closing stagingQueueChannel", e);
        }
    }
}
