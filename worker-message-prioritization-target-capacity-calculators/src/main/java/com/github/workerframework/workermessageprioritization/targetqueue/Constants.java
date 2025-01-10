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

final class Constants
{
    private Constants()
    {
    }

    public static final long TARGET_QUEUE_MAX_LENGTH_FALLBACK = 1000;
    public static final long TARGET_QUEUE_ELIGIBLE_FOR_REFILL_PERCENTAGE_FALLBACK = 10;
    public static final int CURRENT_INSTANCE_FALLBACK = 1;
    public static final int MAX_INSTANCES_FALLBACK = 1;
    public static final long CAPACITY_FALLBACK = 1000;
    public static final TargetQueueSettings FALLBACK_TARGET_QUEUE_SETTINGS = new TargetQueueSettings(
            TARGET_QUEUE_MAX_LENGTH_FALLBACK,
            TARGET_QUEUE_ELIGIBLE_FOR_REFILL_PERCENTAGE_FALLBACK,
            MAX_INSTANCES_FALLBACK,
            CURRENT_INSTANCE_FALLBACK,
            CAPACITY_FALLBACK);
}
