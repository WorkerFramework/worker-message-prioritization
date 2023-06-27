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
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.ShutdownSignalException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;

public class StagingQueueConsumer extends DefaultConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(StagingQueueConsumer.class);
    private final StagingQueueTargetQueuePair stagingQueueTargetQueuePair;
    private boolean cancellationRequested = false;
    private Instant cancellationRequestSentTime;
    private boolean cancelled = false;
    private ShutdownSignalException shutdownSignalException;

    public StagingQueueConsumer(final Channel channel, final StagingQueueTargetQueuePair stagingQueueTargetQueuePair) {
        super(channel);
        this.stagingQueueTargetQueuePair = stagingQueueTargetQueuePair;
    }

    public void setCancellationRequested(final boolean cancellationRequested) {
        if (cancellationRequested) {
            cancellationRequestSentTime = Instant.now();
        } else {
            cancellationRequestSentTime = null;
        }
        this.cancellationRequested = cancellationRequested;
    }

    public boolean isCancellationRequested() {
        return cancellationRequested;
    }

    public Instant getCancellationRequestSentTime() {
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
        LOGGER.debug("handleCancel called for consumer with consumerTag {}", consumerTag);
        cancelled = true;
    }

    @Override
    public void handleCancelOk(final String consumerTag) {
        //Stop tracking that we are consuming from the consumerTag queue
        LOGGER.debug("handleCancelOk called for consumer with consumerTag {}", consumerTag);
        cancelled = true;
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
}
