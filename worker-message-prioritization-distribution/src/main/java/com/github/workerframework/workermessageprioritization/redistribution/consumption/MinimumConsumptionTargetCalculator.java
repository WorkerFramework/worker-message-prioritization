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
import com.github.workerframework.workermessageprioritization.targetqueue.TargetQueueSettingsProvider;
import com.github.workerframework.workermessageprioritization.targetqueue.targetqueuesettings.TargetQueueSettings;

public abstract class MinimumConsumptionTargetCalculator extends ConsumptionTargetCalculatorBase
{
    public MinimumConsumptionTargetCalculator(final TargetQueueSettingsProvider targetQueueSettingsProvider)
    {
        super(targetQueueSettingsProvider);
    }

    @Override
    protected long getTargetQueueCapacity(final Queue targetQueue)
    {
        final TargetQueueSettings targetQueueSettings = getTargetQueueSettings(targetQueue);
        final long targetQueueCapacity = super.getTargetQueueCapacity(targetQueue);
        final long targetQueueCapacityPercentage = targetQueueCapacity * 100 / targetQueueSettings.getMaxLength();

        if (targetQueueCapacityPercentage < targetQueueSettings.getEligibleForRefillPercentage()) {
            return 0;
        } else {
            return targetQueueCapacity;
        }
    }
}
