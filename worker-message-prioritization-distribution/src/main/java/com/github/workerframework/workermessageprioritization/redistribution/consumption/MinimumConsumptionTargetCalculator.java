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
package com.github.workerframework.workermessageprioritization.redistribution.consumption;

import com.github.workerframework.workermessageprioritization.rabbitmq.Queue;
import com.github.workerframework.workermessageprioritization.redistribution.DistributorWorkItem;
import com.github.workerframework.workermessageprioritization.targetqueue.TargetQueueSettingsProvider;
import com.github.workerframework.workermessageprioritization.targetqueue.targetqueuesettings.TargetQueueSettings;

import java.util.Map;

public final class MinimumConsumptionTargetCalculator implements ConsumptionTargetCalculator
{

    private final TargetQueueSettingsProvider targetQueueSettingsProvider;
    private final ConsumptionTargetCalculator consumptionTargetCalculator;

    public MinimumConsumptionTargetCalculator(final TargetQueueSettingsProvider targetQueueSettingsProvider)
    {
        this.targetQueueSettingsProvider = targetQueueSettingsProvider;
        this.consumptionTargetCalculator = new EqualConsumptionTargetCalculator(targetQueueSettingsProvider);
    }

    @Override
    public Map<Queue, Long> calculateConsumptionTargets(final DistributorWorkItem distributorWorkItem)
    {
        final TargetQueueSettings targetQueueSettings = targetQueueSettingsProvider.get(distributorWorkItem.getTargetQueue());

        final Map<Queue, Long> stagingQueues = consumptionTargetCalculator.calculateConsumptionTargets(distributorWorkItem);

        final long lastKnownTargetQueueLength = distributorWorkItem.getTargetQueue().getMessages();

        final long consumptionTarget = targetQueueSettings.getMaxLength() - lastKnownTargetQueueLength;

        final long sourceQueueConsumptionTarget;

        if (consumptionTarget < targetQueueSettings.getEligibleForRefill()) {
            sourceQueueConsumptionTarget = 0;
            stagingQueues.replaceAll((k, v) -> v = sourceQueueConsumptionTarget);
        }
        return stagingQueues;
    }
}
