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

import com.github.workerframework.workermessageprioritization.rabbitmq.Queue;
import com.google.inject.Inject;

public abstract class CapacityCalculatorBase {

    private final CapacityCalculatorBase next;

    @Inject
    public CapacityCalculatorBase(final CapacityCalculatorBase next) {
        this.next = next;
    }

    public TargetQueueSettings refine(final Queue targetQueue, final TargetQueueSettings targetQueueSettings) {
        final TargetQueueSettings refinedTargetQueueSettings = refineInternal(targetQueue, targetQueueSettings);
        if(next != null) {
            return next.refine(targetQueue, refinedTargetQueueSettings);
        }
        return refinedTargetQueueSettings;
    }

    protected abstract TargetQueueSettings refineInternal(final Queue queue, final TargetQueueSettings targetQueueSettings);
}
