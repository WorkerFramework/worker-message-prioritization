package com.github.workerframework.workermessageprioritization.redistribution.consumption;

import com.github.workerframework.workermessageprioritization.rabbitmq.Queue;
import com.github.workerframework.workermessageprioritization.targetqueue.TargetQueueSettingsProvider;
import com.github.workerframework.workermessageprioritization.targetqueue.targetqueuesettings.TargetQueueSettings;

public abstract class ConsumptionTargetCalculatorBase implements ConsumptionTargetCalculator
{
    private final TargetQueueSettingsProvider targetQueueSettingsProvider;

    public ConsumptionTargetCalculatorBase(final TargetQueueSettingsProvider targetQueueSettingsProvider)
    {
        this.targetQueueSettingsProvider = targetQueueSettingsProvider;
    }

    protected long getTargetQueueCapacity(final Queue targetQueue)
    {
        return getTargetQueueSettings(targetQueue).getMaxLength();
    }

    protected TargetQueueSettings getTargetQueueSettings(final Queue targetQueue)
    {
        return targetQueueSettingsProvider.get(targetQueue);
    }
}
