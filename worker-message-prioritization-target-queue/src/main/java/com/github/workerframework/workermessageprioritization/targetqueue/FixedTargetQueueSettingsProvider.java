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

import com.github.workerframework.workermessageprioritization.rabbitmq.Queue;
import com.github.workerframework.workermessageprioritization.targetqueue.targetqueuesettings.TargetQueueSettings;

/**
 * An example implementation to be used as a reference for further, real world, implementations
 */
public final class FixedTargetQueueSettingsProvider implements TargetQueueSettingsProvider
{
    private static final long QUEUE_MAX_LENGTH = 1000;
    private static final long QUEUE_ELIGIBLE_FOR_REFILL = 20;

    @Override
    public TargetQueueSettings get(final Queue targetQueue)
    {
        return new TargetQueueSettings(QUEUE_MAX_LENGTH - targetQueue.getMessages(), QUEUE_ELIGIBLE_FOR_REFILL);
    }
}
