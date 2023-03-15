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
package com.github.workerframework.workermessageprioritization.redistribution;

import com.github.workerframework.workermessageprioritization.rabbitmq.Queue;

import java.util.Collections;
import java.util.Set;

/**
 * Represents the relationship between staging queues and their target queue.
 * A MessageDistributor implementation will schedule the transfer of messages on a staging queue to a target queue.
 */
public class DistributorWorkItem {
    
    private final Queue targetQueue;
    private final Set<Queue> stagingQueues;

    public DistributorWorkItem(final Queue targetQueue, final Set<Queue> stagingQueues) {
        this.targetQueue = targetQueue;
        this.stagingQueues = Collections.unmodifiableSet(stagingQueues);
    }

    public Queue getTargetQueue() {
        return targetQueue;
    }

    public Set<Queue> getStagingQueues() {
        return stagingQueues;
    }
}
