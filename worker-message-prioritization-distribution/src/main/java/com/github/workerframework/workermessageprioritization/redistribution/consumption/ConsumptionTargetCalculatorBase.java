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

public abstract class ConsumptionTargetCalculatorBase implements ConsumptionTargetCalculator
{
    private final TargetQueueSettingsProvider targetQueueSettingsProvider;

    public ConsumptionTargetCalculatorBase(final TargetQueueSettingsProvider targetQueueSettingsProvider)
    {
        this.targetQueueSettingsProvider = targetQueueSettingsProvider;
    }

    protected long getTargetQueueCapacity(final Queue targetQueue)
    {
        return getTargetQueueSettings(targetQueue).getMaxLength() - targetQueue.getMessages();
    }

    protected TargetQueueSettings getTargetQueueSettings(final Queue targetQueue)
    {
        return targetQueueSettingsProvider.get(targetQueue);
    }
}