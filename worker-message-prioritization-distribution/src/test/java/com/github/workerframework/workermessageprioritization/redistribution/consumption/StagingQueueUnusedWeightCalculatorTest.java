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
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ErrorCollector;

import java.util.Set;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

// This tests the calculateStagingQueueUnusedWeight function. This method calculates the amount of unused weight per staging queue and
// returns this value divided by the total weight of queues that have a queue larger than the capacity provided.
// The idea is that if there are smaller staging queues that do not use the full capacity they are provided, this space can be
// redistributed around the other larger staging queues.
// If there is no unused weight a value of 0 will be returned.
// If there is unused weight for all the staging queues, each staging queue will get capacity for its whole queue as the sum of all
// these queues does not make up the capacity of target queue available .
public class StagingQueueUnusedWeightCalculatorTest {
    @Rule
    public final ErrorCollector collector = new ErrorCollector();

    @Test
    public void calculateStagingQueueUnusedConsumptionWithSmallEqualQueuesTest() {
        final Queue q1 = getQueue("sq1", 10);
        final Queue q2 = getQueue("sq2", 10);

        final Set<Queue> stagingQueues = new HashSet<>(Arrays.asList(q1, q2));
        final Map<String, Double> stagingQueueWeights = new HashMap<>();
        stagingQueues.forEach(queue -> stagingQueueWeights.put(queue.getName(), 1D));
        final DistributorWorkItem distributorWorkItem = mock(DistributorWorkItem.class);
        when(distributorWorkItem.getStagingQueues()).thenReturn(stagingQueues);
        final long targetQueueCapacity = 1000;
        final StagingQueueUnusedWeightCalculator stagingQueueUnusedWeightCalculator =
                new StagingQueueUnusedWeightCalculator();

        final double weightIncrease =
                stagingQueueUnusedWeightCalculator.calculateStagingQueueUnusedWeight(distributorWorkItem,
                        targetQueueCapacity, stagingQueueWeights.size(), stagingQueueWeights);

        assertEquals("Weight increase should be equal to the totalStagingQueueWeight as in the case that the " +
                    "messages do not make up the capacity available, the weight will be set to the total of the all the weights to " +
                    "ensure when actualNumberOfMessagesToConsumeFromStagingQueue is found, the minimum set is " +
                    "the total length of the staging queue.",
                2D, weightIncrease, 0D);
    }

    @Test
    public void calculateStagingQueueUnusedConsumptionWithLargeEqualQueuesTest() {
        final Queue q1 = getQueue("sq1", 1000);
        final Queue q2 = getQueue("sq2", 1000);

        final Set<Queue> stagingQueues = new HashSet<>(Arrays.asList(q1, q2));
        final Map<String, Double> stagingQueueWeights = new HashMap<>();
        stagingQueues.forEach(queue -> stagingQueueWeights.put(queue.getName(), 1D));
        final DistributorWorkItem distributorWorkItem = mock(DistributorWorkItem.class);
        when(distributorWorkItem.getStagingQueues()).thenReturn(stagingQueues);
        final long targetQueueCapacity = 1000;
        final StagingQueueUnusedWeightCalculator stagingQueueUnusedWeightCalculator =
                new StagingQueueUnusedWeightCalculator();

        final double weightIncrease =
                stagingQueueUnusedWeightCalculator.calculateStagingQueueUnusedWeight
                        (distributorWorkItem, targetQueueCapacity, stagingQueueWeights.size(),stagingQueueWeights);
        assertEquals("Result should be zero as all queues have been set greater than available " +
                "space in target queue. Therefore each queue can use an equal amount of space and nothing is leftover",
                0D, weightIncrease, 0D);
    }

    @Test
    public void calculateStagingQueueWeightIncreaseWith2UnequalQueuesTest() {

        final Queue q1 = getQueue("sq1", 200);
        final Queue q2 = getQueue("sq2", 50);

        final Set<Queue> stagingQueues = new HashSet<>(Arrays.asList(q1, q2));
        final Map<String, Double> stagingQueueWeights = new HashMap<>();
        stagingQueues.forEach(queue -> stagingQueueWeights.put(queue.getName(), 1D));
        final DistributorWorkItem distributorWorkItem = mock(DistributorWorkItem.class);
        when(distributorWorkItem.getStagingQueues()).thenReturn(stagingQueues);
        final long targetQueueCapacity = 200;
        final StagingQueueUnusedWeightCalculator stagingQueueCalculateConsumptionWeightIncrease =
                new StagingQueueUnusedWeightCalculator();

        final double weightIncrease =
                stagingQueueCalculateConsumptionWeightIncrease.calculateStagingQueueUnusedWeight(distributorWorkItem,
                        targetQueueCapacity, stagingQueueWeights.size(),stagingQueueWeights);

        assertEquals("Result should be 0.5 as one queue has half of the queue length available," +
                " leaving the other half of the space unused. Adding 0.5 to each weight will provide the " +
                "larger queue with 150 message spaces.",
                0.5D, weightIncrease, 0D);

    }

    @Test
    public void calculateStagingQueueUnusedConsumptionWithMultipleUnequalQueuesTest() {

        final Queue q1 = getQueue("sq1", 1000);
        final Queue q2 = getQueue("sq2", 50);
        final Queue q3 = getQueue("sq3", 10);
        final Queue q4 = getQueue("sq4", 1);
        final Queue q5 = getQueue("sq4", 90);
        final Queue q6 = getQueue("sq4", 300);
        final Queue q7 = getQueue("sq4", 400);
        final Queue q8 = getQueue("sq4", 70000);
        final Queue q9 = getQueue("sq4", 300);
        final Queue q10 = getQueue("sq4", 50);

        final Set<Queue> stagingQueues = new HashSet<>(Arrays.asList(q1, q2, q3, q4, q5, q6, q7, q8, q9, q10));

        final Map<String, Double> stagingQueueWeights = new HashMap<>();
        stagingQueues.forEach(queue -> stagingQueueWeights.put(queue.getName(), 1D));

        final DistributorWorkItem distributorWorkItem = mock(DistributorWorkItem.class);
        when(distributorWorkItem.getStagingQueues()).thenReturn(stagingQueues);

        final long targetQueueCapacity = 1000;
        final StagingQueueUnusedWeightCalculator stagingQueueUnusedWeightCalculator =
                new StagingQueueUnusedWeightCalculator();

        final double weightIncrease =
                stagingQueueUnusedWeightCalculator.calculateStagingQueueUnusedWeight
                        (distributorWorkItem, targetQueueCapacity, stagingQueues.size(),stagingQueueWeights);

        assertEquals("Result should be the sum of leftover weight space. " +
                "Eg. 1000/10 = 100. q5 only has 90 messages, therefore 0.1 out of the weight 1 is leftover, and so on.",
                0.598D, weightIncrease, 0.002D);

    }

    Queue getQueue(final String name, final long messages) {
        final Queue queue = new Queue();
        queue.setName(name);
        queue.setMessages(messages);
        return queue;
    }

}
