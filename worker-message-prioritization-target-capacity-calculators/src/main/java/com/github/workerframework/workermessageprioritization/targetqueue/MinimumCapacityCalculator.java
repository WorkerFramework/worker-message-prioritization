/*
 * Copyright 2022-2024 Open Text.
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MinimumCapacityCalculator extends CapacityCalculatorBase {
    private static final Logger TUNED_TARGET_LOGGER = LoggerFactory.getLogger("TUNED_TARGET");

    public MinimumCapacityCalculator(CapacityCalculatorBase next) {
        super(next);
    }

    @Override
    protected TargetQueueSettings refineInternal(final Queue targetQueue, final TargetQueueSettings targetQueueSettings) {

        TUNED_TARGET_LOGGER.debug("Calculating the minimum capacity. Current length: {}", targetQueueSettings.getCurrentMaxLength());

        final long targetQueueCapacity = Math.max(0, targetQueueSettings.getCurrentMaxLength() - targetQueue.getMessages());

        TUNED_TARGET_LOGGER.debug("Target queue capacity: {}", targetQueueCapacity);

        final long targetQueueCapacityPercentage = targetQueueCapacity * 100 / targetQueueSettings.getCurrentMaxLength();

        TUNED_TARGET_LOGGER.debug("Target queue percentage: {}", targetQueueCapacityPercentage);

        if (targetQueueCapacityPercentage < targetQueueSettings.getEligibleForRefillPercentage()) {
            return new TargetQueueSettings(targetQueueSettings.getCurrentMaxLength(),
                    targetQueueSettings.getEligibleForRefillPercentage(), targetQueueSettings.getMaxInstances(),
                    targetQueueSettings.getCurrentInstances(), 0);
        } else {
            return new TargetQueueSettings(targetQueueSettings.getCurrentMaxLength(),
                    targetQueueSettings.getEligibleForRefillPercentage(), targetQueueSettings.getMaxInstances(),
                    targetQueueSettings.getCurrentInstances(), targetQueueCapacity);
        }
    }
}
