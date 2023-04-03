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
