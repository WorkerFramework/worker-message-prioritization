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
package com.github.workerframework.workermessageprioritization.redistribution.consumption;

import com.github.workerframework.workermessageprioritization.rabbitmq.Queue;
import com.github.workerframework.workermessageprioritization.redistribution.DistributorWorkItem;

import java.util.Map;

public interface ConsumptionTargetCalculator {
    /**
     * Calculate how many messages should be consumed from the staging queues
     * @param distributorWorkItem The target queue and the staging queues containing messages to be sent to the target 
     *                            queue.
     * @return A map containing the staging queues for a target queue and how many messages to consume from each 
     * staging queue
     */
    Map<Queue, Long> calculateConsumptionTargets(final DistributorWorkItem distributorWorkItem);
    
}
