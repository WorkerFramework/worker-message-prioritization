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
package com.github.workerframework.workermessageprioritization.targetqueue.targetqueuesettings;

public final class TargetQueueSettings
{
    private final long maxLength;
    private final long eligibleForRefillPercentage;

    public TargetQueueSettings(final long maxLength, final long eligibleForRefillPercentage)
    {
        this.maxLength = maxLength;
        this.eligibleForRefillPercentage = eligibleForRefillPercentage;
    }

    public long getMaxLength()
    {
        return this.maxLength;
    }

    public long getEligibleForRefillPercentage()
    {
        return eligibleForRefillPercentage;
    }
}
