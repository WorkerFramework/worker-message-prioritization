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
package com.github.workerframework.workermessageprioritization.targetrefill;

import com.github.workerframework.workermessageprioritization.rabbitmq.Queue;

/**
 * Obtain the minimum number of messages which the target queue is eligible to be refilled with
 */
public interface TargetQueueRefillProvider
{

    /**
     * Obtain threshold of messages needed to refill target queue
     *
     * @param targetQueue The target queue to obtain threshold
     * @return The number of messages needed to refill the queue
     */
    long get(final Queue targetQueue);
}
