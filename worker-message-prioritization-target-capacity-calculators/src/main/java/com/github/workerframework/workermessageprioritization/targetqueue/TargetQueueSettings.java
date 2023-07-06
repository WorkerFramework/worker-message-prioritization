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

public final class TargetQueueSettings
{
    private long currentMaxLength;
    private final long eligibleForRefillPercentage;
    private final double currentInstances;
    private final double maxInstances;

    public TargetQueueSettings(final long currentMaxLength, final long eligibleForRefillPercentage,
                               final double maxInstances, final double currentInstances)
    {
        this.currentMaxLength = currentMaxLength;
        this.eligibleForRefillPercentage = eligibleForRefillPercentage;
        this.currentInstances = currentInstances;
        this.maxInstances = maxInstances;
    }

    public long getCurrentMaxLength()
    {
        return this.currentMaxLength;
    }

    public void setCurrentMaxLength(final long currentMaxLength){
        this.currentMaxLength = currentMaxLength;
    }

    public long getEligibleForRefillPercentage()
    {
        return eligibleForRefillPercentage;
    }

    public double getCurrentInstances() {
        return currentInstances;
    }

    public double getMaxInstances() {
        return maxInstances;
    }
}
