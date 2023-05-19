package com.github.workerframework.workermessageprioritization.targetqueue.targetqueuesettings;

public final class TargetQueueSettings
{
    private long maxLength;
    private long eligibleForRefill;

    public TargetQueueSettings(final long maxLength, final long eligibleForRefill)
    {
        this.maxLength = maxLength;
        this.eligibleForRefill = eligibleForRefill;
    }

    public long getMaxLength()
    {
        return this.maxLength;
    }

    public long getEligibleForRefill()
    {
        return eligibleForRefill;
    }
}
