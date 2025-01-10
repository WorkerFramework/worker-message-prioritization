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
package com.github.workerframework.workermessageprioritization.targetqueue;

import com.github.workerframework.workermessageprioritization.rabbitmq.Queue;
import com.google.inject.Inject;

public class TunedCapacityCalculator extends CapacityCalculatorBase {

    private final TunedTargetQueueLengthProvider tunedTargetQueueLengthProvider;

    @Inject
    public TunedCapacityCalculator(final TunedTargetQueueLengthProvider tunedTargetQueueLengthProvider,
                                   final CapacityCalculatorBase next) {
        super(next);
        this.tunedTargetQueueLengthProvider = tunedTargetQueueLengthProvider;
    }

    @Override
    protected TargetQueueSettings refineInternal(final Queue targetQueue, final TargetQueueSettings targetQueueSettings) {

        final long tunedTargetMaxQueueLength = tunedTargetQueueLengthProvider.getTunedTargetQueueLength(targetQueue.getName(),
                targetQueueSettings);

        return new TargetQueueSettings(tunedTargetMaxQueueLength,
                targetQueueSettings.getEligibleForRefillPercentage(), targetQueueSettings.getMaxInstances(),
                targetQueueSettings.getCurrentInstances(), targetQueueSettings.getCapacity());

    }
}
