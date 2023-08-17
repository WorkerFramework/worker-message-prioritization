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

public class MinimumCapacityCalculator extends CapacityCalculatorBase {
    public MinimumCapacityCalculator(CapacityCalculatorBase next) {
        super(next);
    }

    @Override
    protected TargetQueueSettings refineInternal(final Queue targetQueue, final TargetQueueSettings targetQueueSettings) {

        final long targetQueueCapacity = Math.max(0, targetQueueSettings.getCurrentMaxLength() - targetQueue.getMessages());

        final long targetQueueCapacityPercentage = targetQueueCapacity * 100 / targetQueueSettings.getCurrentMaxLength();

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
