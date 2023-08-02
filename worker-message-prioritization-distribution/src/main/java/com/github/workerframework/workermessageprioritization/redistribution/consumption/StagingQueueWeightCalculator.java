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
package com.github.workerframework.workermessageprioritization.redistribution.consumption;

import com.github.workerframework.workermessageprioritization.rabbitmq.Queue;
import com.github.workerframework.workermessageprioritization.redistribution.DistributorWorkItem;

import java.util.HashMap;
import java.util.Map;

public class StagingQueueWeightCalculator {
    long stagingQueueWeight = 0;
    final DistributorWorkItem distributorWorkItem;

    public StagingQueueWeightCalculator(DistributorWorkItem distributorWorkItem) {
        this.distributorWorkItem = distributorWorkItem;
    }

    public double calculateTotalStagingQueueWeight(){
        for (final Queue stagingQueue : distributorWorkItem.getStagingQueues()) {
            stagingQueueWeight += 1;
            //get these from the settings service.
        }
        return stagingQueueWeight;
    }

    public Map<Queue, Double> getStagingQueueWeights(){

        final Map<Queue, Double> stagingQueueWeights = new HashMap<>();

        for (final Queue stagingQueue : distributorWorkItem.getStagingQueues()) {
            //call to the settings service to get the weight of a queue
            stagingQueueWeights.put(stagingQueue, 1D);
        }

        return stagingQueueWeights;
    }
}
