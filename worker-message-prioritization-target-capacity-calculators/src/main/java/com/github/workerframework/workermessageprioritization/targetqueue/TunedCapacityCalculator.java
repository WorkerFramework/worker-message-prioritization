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
package com.github.workerframework.workermessageprioritization.targetqueue;

import com.github.workerframework.workermessageprioritization.rabbitmq.Queue;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TunedCapacityCalculator extends CapacityCalculatorBase {

    private final TunedTargetQueueLengthProvider tunedTargetQueueLengthProvider;
    private static final Logger TUNED_TARGET_LOGGER = LoggerFactory.getLogger("TUNED_TARGET");

    @Inject
    public TunedCapacityCalculator(final TunedTargetQueueLengthProvider tunedTargetQueueLengthProvider,
                                   final CapacityCalculatorBase next) {
        super(next);
        this.tunedTargetQueueLengthProvider = tunedTargetQueueLengthProvider;
    }

    @Override
    protected TargetQueueSettings refineInternal(final Queue targetQueue, final TargetQueueSettings targetQueueSettings) {

        TUNED_TARGET_LOGGER.info("Calculating the tuned target length. Current length:" + targetQueueSettings.getCurrentMaxLength() );

        final long tunedTargetMaxQueueLength = tunedTargetQueueLengthProvider.getTunedTargetQueueLength(targetQueue.getName(),
                targetQueueSettings);

        TUNED_TARGET_LOGGER.info("After tuning. Current length:" + targetQueueSettings.getCurrentMaxLength() );

        return new TargetQueueSettings(tunedTargetMaxQueueLength,
                targetQueueSettings.getEligibleForRefillPercentage(), targetQueueSettings.getMaxInstances(),
                targetQueueSettings.getCurrentInstances(), targetQueueSettings.getCapacity());

    }
}
