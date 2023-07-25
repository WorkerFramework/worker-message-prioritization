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
import com.github.workerframework.workermessageprioritization.targetqueue.TargetQueueSettings;
import com.github.workerframework.workermessageprioritization.targetqueue.TunedTargetQueueLengthProvider;
import com.google.common.base.Strings;

public abstract class ConsumptionTargetCalculatorBase implements ConsumptionTargetCalculator
{
    private final TargetQueueSettingsProvider targetQueueSettingsProvider;
    private final TunedTargetQueueLengthProvider tunedTargetQueueLengthProvider;

    public ConsumptionTargetCalculatorBase(final TargetQueueSettingsProvider targetQueueSettingsProvider,
                                           final TunedTargetQueueLengthProvider tunedTargetQueueLengthProvider)
    {
        this.targetQueueSettingsProvider = targetQueueSettingsProvider;
        this.tunedTargetQueueLengthProvider = tunedTargetQueueLengthProvider;
    }

    protected long getTargetQueueCapacity(final Queue targetQueue)
    {
        int minTargetQueueLength = !Strings.isNullOrEmpty(System.getenv("CAF_MIN_TARGET_QUEUE_LENGTH")) ?
                Integer.parseInt(System.getenv("CAF_MIN_TARGET_QUEUE_LENGTH")) : 100;
        int maxTargetQueueLength = !Strings.isNullOrEmpty(System.getenv("CAF_MAX_TARGET_QUEUE_LENGTH")) ?
                Integer.parseInt(System.getenv("CAF_MIN_TARGET_QUEUE_LENGTH")) : 10000000;
        final long tunedTargetMaxQueueLength = tunedTargetQueueLengthProvider.getTunedTargetQueueLength(targetQueue.getName(),
                minTargetQueueLength, maxTargetQueueLength, targetQueueSettingsProvider.get(targetQueue));
        getTargetQueueSettings(targetQueue).setCurrentMaxLength(tunedTargetMaxQueueLength);
        return Math.max(0, getTargetQueueSettings(targetQueue).getCurrentMaxLength() - targetQueue.getMessages());
    }

    protected TargetQueueSettings getTargetQueueSettings(final Queue targetQueue)
    {
        return targetQueueSettingsProvider.get(targetQueue);
    }
}
