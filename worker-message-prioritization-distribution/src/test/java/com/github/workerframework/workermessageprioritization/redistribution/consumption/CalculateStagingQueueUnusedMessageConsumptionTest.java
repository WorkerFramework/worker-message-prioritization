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

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

// This tests the calculateStagingQueueUnusedWeight function. This method calculates the amount of unused weight per staging queue and
// returns this value divided by the total weight of queues that have a queue larger than the capacity provided.
// The idea is that if there are smaller staging queues that do not use the full capacity they are provided, this space can be
// redistributed around the other larger staging queues.
// If there is no unused weight a value of 0 will be returned. Equally, if there is unused weight for all the staging queues, a value
// of 0 will be returned, as the unused weight is not needed for any other queues.
public class CalculateStagingQueueUnusedMessageConsumptionTest {
    @Rule
    public final ErrorCollector collector = new ErrorCollector();

    @Test
    public void calculateStagingQueueUnusedConsumptionWithSmallEqualQueuesTest() {
        final Queue q1 = new Queue();
        final Queue q2 = new Queue();
        final Set<Queue> stagingQueues = new HashSet<>(Arrays.asList(q1, q2));
        stagingQueues.forEach((queue -> queue.setMessages(10)));
        final DistributorWorkItem distributorWorkItem = mock(DistributorWorkItem.class);
        when(distributorWorkItem.getStagingQueues()).thenReturn(stagingQueues);
        final long targetQueueCapacity = 1000;
        final StagingQueueUnusedMessageConsumptionCalculator stagingQueueUnusedMessageConsumptionCalculator =
                new StagingQueueUnusedMessageConsumptionCalculator(distributorWorkItem);

        final double weightIncrease =
                stagingQueueUnusedMessageConsumptionCalculator.calculateStagingQueueUnusedWeight(targetQueueCapacity, 2D);

        collector.checkThat("Result should be zero as all queues have been set less than the targetQueueCapacity offered to " +
                        "the queue. This means no leftover is required as there is no staging queue to use the leftover.", 0D,
                equalTo(weightIncrease));
    }

    @Test
    public void calculateStagingQueueUnusedConsumptionWithLargeEqualQueuesTest() {
        final Queue q1 = new Queue();
        final Queue q2 = new Queue();
        final Set<Queue> stagingQueues = new HashSet<>(Arrays.asList(q1, q2));
        stagingQueues.forEach((queue -> queue.setMessages(1000)));
        final DistributorWorkItem distributorWorkItem = mock(DistributorWorkItem.class);
        when(distributorWorkItem.getStagingQueues()).thenReturn(stagingQueues);
        final long targetQueueCapacity = 1000;
        final StagingQueueUnusedMessageConsumptionCalculator stagingQueueUnusedMessageConsumptionCalculator =
                new StagingQueueUnusedMessageConsumptionCalculator(distributorWorkItem);

        final double weightIncrease =
                stagingQueueUnusedMessageConsumptionCalculator.calculateStagingQueueUnusedWeight(targetQueueCapacity, 2D);
        collector.checkThat("Result should be zero as all queues have been set greater than available space in target queue. " +
                        "Therefore each queue can use an equal amount of space and nothing is leftover", 0D,
                equalTo(weightIncrease));
    }

    @Test
    public void calculateStagingQueueWeightIncreaseWith2UnequalQueuesTest() {
        final Queue q1 = new Queue();
        q1.setMessages(200);
        final Queue q2 = new Queue();
        q2.setMessages(50);
        final Set<Queue> stagingQueues = new HashSet<>(Arrays.asList(q1, q2));
        final DistributorWorkItem distributorWorkItem = mock(DistributorWorkItem.class);
        when(distributorWorkItem.getStagingQueues()).thenReturn(stagingQueues);
        final long targetQueueCapacity = 200;
        final StagingQueueUnusedMessageConsumptionCalculator stagingQueueCalculateConsumptionWeightIncrease = new StagingQueueUnusedMessageConsumptionCalculator(distributorWorkItem);

        final double weightIncrease =
                stagingQueueCalculateConsumptionWeightIncrease.calculateStagingQueueUnusedWeight(targetQueueCapacity, 2D);

        collector.checkThat("Result should be 0.5 as one queue has half of the queue length available, leaving the other half of the " +
                        "space unused. Adding 0.5 to each weight will provide the larger queue with 150 message spaces.", 0.5D,
                equalTo(weightIncrease));

    }

    @Test
    public void calculateStagingQueueUnusedConsumptionWithMultipleUnequalQueuesTest() {
        final Queue q1 = new Queue();
        final Queue q2 = new Queue();
        final Queue q3 = new Queue();
        final Queue q4 = new Queue();
        final Queue q5 = new Queue();
        final Queue q6 = new Queue();
        final Queue q7 = new Queue();
        final Queue q8 = new Queue();
        final Queue q9 = new Queue();
        final Queue q10 = new Queue();
        q1.setMessages(1000);
        q2.setMessages(50);
        q3.setMessages(10);
        q4.setMessages(1);
        q5.setMessages(90);
        q6.setMessages(300);
        q7.setMessages(400);
        q8.setMessages(70000);
        q9.setMessages(300);
        q10.setMessages(50);

        final Set<Queue> stagingQueues = new HashSet<>(Arrays.asList(q1, q2, q3, q4, q5, q6, q7, q8, q9, q10));
        final DistributorWorkItem distributorWorkItem = mock(DistributorWorkItem.class);
        when(distributorWorkItem.getStagingQueues()).thenReturn(stagingQueues);

        final long targetQueueCapacity = 1000;
        final StagingQueueUnusedMessageConsumptionCalculator stagingQueueUnusedMessageConsumptionCalculator =
                new StagingQueueUnusedMessageConsumptionCalculator(distributorWorkItem);

        final double weightIncrease =
                stagingQueueUnusedMessageConsumptionCalculator
                        .calculateStagingQueueUnusedWeight(targetQueueCapacity, stagingQueues.size());

        collector.checkThat("Result should be the sum of leftover weight space. Eg. 1000/10 = 100. q5 only has 90 messages, " +
                        "therefore 0.1 out of the weight 1 is leftover.", 0.598D,
                equalTo(weightIncrease));

    }

}
