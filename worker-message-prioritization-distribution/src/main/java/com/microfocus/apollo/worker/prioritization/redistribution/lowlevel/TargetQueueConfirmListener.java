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

import com.rabbitmq.client.ConfirmListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class TargetQueueConfirmListener implements ConfirmListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(TargetQueueConfirmListener.class);

    private final StagingQueueTargetQueuePair stagingQueueTargetQueuePair;

    public TargetQueueConfirmListener(final StagingQueueTargetQueuePair stagingQueueTargetQueuePair) {
        this.stagingQueueTargetQueuePair = stagingQueueTargetQueuePair;
    }
    
    @Override
    public void handleAck(long deliveryTag, boolean multiple) throws IOException {
        stagingQueueTargetQueuePair.handleDeliveryToTargetQueueAck(deliveryTag, multiple);
    }

    @Override
    public void handleNack(long deliveryTag, boolean multiple) throws IOException {
        stagingQueueTargetQueuePair.handleDeliveryToTargetQueueNack(deliveryTag, multiple);
    }
    
}
