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

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.ShutdownSignalException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class StagingQueueConsumer extends DefaultConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(StagingQueueConsumer.class);
    private final StagingQueueTargetQueuePair stagingQueueTargetQueuePair;

    private boolean active;
    private ShutdownSignalException shutdownSignalException;

    public StagingQueueConsumer(final Channel channel, final StagingQueueTargetQueuePair stagingQueueTargetQueuePair) {
        super(channel);
        this.stagingQueueTargetQueuePair = stagingQueueTargetQueuePair;
    }

    public boolean isActive() {
        return active;
    }

    public void setActive(final boolean active) {
        this.active = active;
    }
    
    public ShutdownSignalException getShutdownSignalException() {
        return shutdownSignalException;
    }
    
    @Override
    public void handleCancel(final String consumerTag) throws IOException {
        //Stop tracking that we are consuming from the consumerTag queue
        active = false;
    }

    @Override
    public void handleShutdownSignal(final String consumerTag, final ShutdownSignalException sig) {
        //Connection lost, give up
        shutdownSignalException = sig;
        active = false;
    }

    @Override
    public void handleDelivery(final String consumerTag, final Envelope envelope, 
                               final AMQP.BasicProperties properties, final byte[] body) throws IOException {
        stagingQueueTargetQueuePair.publishToTarget(consumerTag, envelope, properties, body);

    }
}
