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
package com.github.workerframework.workermessageprioritization.rerouting;

import com.github.workerframework.workermessageprioritization.rabbitmq.Queue;
import com.rabbitmq.client.Channel;

import java.io.IOException;
import java.util.HashSet;

public class StagingQueueCreator {
    
    private final HashSet<String> declaredQueues = new HashSet<>();
    private final Channel channel;

    public StagingQueueCreator(final Channel channel) {

        this.channel = channel;
    }

    public void createStagingQueue(final Queue targetQueue, final String stagingQueueName) throws IOException {
        
        if(declaredQueues.contains(stagingQueueName)) {
            return;
        }

        channel.queueDeclare(
            stagingQueueName,
            targetQueue.isDurable(),
            targetQueue.isExclusive(),
            targetQueue.isAuto_delete(),
            targetQueue.getArguments());

        declaredQueues.add(stagingQueueName);
    }
}
