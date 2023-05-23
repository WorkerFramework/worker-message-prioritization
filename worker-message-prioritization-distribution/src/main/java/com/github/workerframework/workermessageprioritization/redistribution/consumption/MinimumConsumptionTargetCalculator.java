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
