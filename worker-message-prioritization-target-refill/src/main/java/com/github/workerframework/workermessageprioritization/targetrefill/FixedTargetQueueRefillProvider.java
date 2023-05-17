package com.github.workerframework.workermessageprioritization.targetrefill;

import com.github.workerframework.workermessageprioritization.rabbitmq.Queue;

public final class FixedTargetQueueRefillProvider implements TargetQueueRefillProvider
{
    private static final long ELIGIBLE_FOR_REFILL = 50;

    @Override
    public long get(final Queue targetQueue)
    {
        return ELIGIBLE_FOR_REFILL;
    }
}
