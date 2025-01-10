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

import com.google.common.base.MoreObjects;

public class TargetQueueSettings
{
    private long currentMaxLength;
    private final long eligibleForRefillPercentage;
    private final double currentInstances;
    private final double maxInstances;
    private final long capacity;

    public TargetQueueSettings(final long currentMaxLength, final long eligibleForRefillPercentage,
                               final double maxInstances, final double currentInstances, final long capacity)
    {
        this.currentMaxLength = currentMaxLength;
        this.eligibleForRefillPercentage = eligibleForRefillPercentage;
        this.currentInstances = currentInstances;
        this.maxInstances = maxInstances;
        this.capacity = capacity;
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

    public long getCapacity(){
        return capacity;
    }

    @Override
    public String toString()
    {
        return MoreObjects.toStringHelper(this)
                .add("currentMaxLength", currentMaxLength)
                .add("eligibleForRefillPercentage", eligibleForRefillPercentage)
                .add("currentInstances", currentInstances)
                .add("maxInstances", maxInstances)
                .add("capacity", capacity)
                .toString();
    }
}
