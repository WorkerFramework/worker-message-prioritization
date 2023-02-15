/*
 * Copyright 2022-2023 Micro Focus or one of its affiliates.
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
package com.github.workerframework.workermessageprioritization.rerouting.reroutedeciders;

import com.github.workerframework.workermessageprioritization.rabbitmq.Queue;
import com.github.workerframework.workermessageprioritization.targetcapacitycalculators.TargetQueueCapacityProvider;

/**
 * A {@link RerouteDecider} that reroutes a message to a staging queue if the target queue does not have capacity for it.
 */
public class TargetQueueCapacityRerouteDecider implements RerouteDecider
{
    private final TargetQueueCapacityProvider targetQueueCapacityProvider;

    public TargetQueueCapacityRerouteDecider(final TargetQueueCapacityProvider targetQueueCapacityProvider)
    {
        this.targetQueueCapacityProvider = targetQueueCapacityProvider;
    }

    @Override
    public boolean shouldReroute(final Queue targetQueue)
    {
        return targetQueue.getMessages() >= targetQueueCapacityProvider.get(targetQueue);
    }
}
