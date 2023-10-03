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
import com.github.workerframework.workermessageprioritization.targetqueue.CapacityCalculatorBase;
import com.github.workerframework.workermessageprioritization.targetqueue.TargetQueueSettings;
import com.github.workerframework.workermessageprioritization.targetqueue.TargetQueueSettingsProvider;
import org.junit.Test;

import java.util.Set;
import java.util.HashSet;
import java.util.HashMap;
import java.util.Arrays;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public final class FastLaneConsumptionTargetCalculatorTest {

    @Test
    public void calculateCapacityAvailableFor1LargeAnd1SmallStagingQueueTest() {

        // There 2 queues and a target queue capacity of 1000 messages available
        // Each queue is offered an equal split of 500 messages.
        // The calculateConsumptionTargets method will use the calculateStagingQueueUnusedWeight method to find the staging queue
        // unused weight (this is explained in CalculateStagingQueueUnusedMessageConsumptionTest)
        // The weight returned from calculateStagingQueueUnusedWeight, will be added to each staging queue weight
        // The staging queues that are smaller will be still weighted higher and therefore offered the extra space however will only take
        // the amount they have.
        // The larger queues will then also be offered extra space and will take that.
        // Hence, in this test calculateStagingQueueUnusedWeight found that queue2 was only using 0.1 weight (50 out of the 500 messages
        // provided). This meant 0.9 was available. As queue1 was the only other queue, its weight was increased to 1.9 and queue 1
        // was offered a capacity of 950 messages. Queue 2 weight will also have been increased to 1.9 however because it only has 50
        // messages it will only take 50.

        final Queue targetQueue = getQueue("tq", 1000);

        final Queue q1 = getQueue("sq1", 1000);
        final Queue q2 = getQueue("sq2", 50);

        final Set<Queue> stagingQueues = new HashSet<>(Arrays.asList(q1, q2));
        final DistributorWorkItem distributorWorkItem = mock(DistributorWorkItem.class);
        when(distributorWorkItem.getStagingQueues()).thenReturn(stagingQueues);
        when(distributorWorkItem.getTargetQueue()).thenReturn(targetQueue);

        final TargetQueueSettings targetQueueSettings = new TargetQueueSettings(1000,10,
                1, 1, 1000L);

        final TargetQueueSettingsProvider targetQueueSettingsProvider = mock(TargetQueueSettingsProvider.class);
        when(targetQueueSettingsProvider.get(targetQueue)).thenReturn(targetQueueSettings);

        final long targetQueueCapacity = targetQueueSettings.getCapacity();

        final CapacityCalculatorBase capacityCalculatorBase = mock(CapacityCalculatorBase.class);
        when(capacityCalculatorBase.refine(any(), any())).thenReturn(targetQueueSettings);

        final FastLaneConsumptionTargetCalculator fastLaneConsumptionTargetCalculator =
                new FastLaneConsumptionTargetCalculator(targetQueueSettingsProvider, capacityCalculatorBase,
                        new StagingQueueWeightSettingsProvider());

        final Map<Queue,Long> consumptionTargets =
                fastLaneConsumptionTargetCalculator.calculateConsumptionTargets(distributorWorkItem);
        final long queue1Result = consumptionTargets.get(q1);
        final long queue2Result = consumptionTargets.get(q2);

        assertEquals("Queue 1 has more than 500 messages, therefore it should be offered " +
                "the rest of the capacity that queue 2 does not need",
                950, queue1Result);
        assertEquals("Queue 2 only has less than 500 messages therefore the capacity of the " +
                "target queue it will fill is the size of its staging queue.",
                50, queue2Result);
        assertEquals("Confirm the total target queue capacity has been used",
                targetQueueCapacity, queue1Result+queue2Result);
    }

    @Test
    public void calculateCapacityAvailableFor2LargeStagingQueuesTest() {

        // Capacity of 1000 means 500 for each queue. These queues both have adequate capacity to
        // fill the 500 therefore there is no redistribution required.

        final Queue targetQueue = getQueue("tq", 1000);

        final Queue q1 = getQueue("sq1", 1000);
        final Queue q2 = getQueue("sq2", 10000);

        final Set<Queue> stagingQueues = new HashSet<>(Arrays.asList(q1, q2));
        final DistributorWorkItem distributorWorkItem = mock(DistributorWorkItem.class);
        when(distributorWorkItem.getStagingQueues()).thenReturn(stagingQueues);
        when(distributorWorkItem.getTargetQueue()).thenReturn(targetQueue);

        final TargetQueueSettings targetQueueSettings =
                new TargetQueueSettings(1000,10, 1, 1, 1000L);

        final TargetQueueSettingsProvider targetQueueSettingsProvider = mock(TargetQueueSettingsProvider.class);
        when(targetQueueSettingsProvider.get(targetQueue)).thenReturn(targetQueueSettings);

        final long targetQueueCapacity = targetQueueSettings.getCapacity();

        final CapacityCalculatorBase capacityCalculatorBase = mock(CapacityCalculatorBase.class);
        when(capacityCalculatorBase.refine(any(), any())).thenReturn(targetQueueSettings);

        final FastLaneConsumptionTargetCalculator fastLaneConsumptionTargetCalculator =
                new FastLaneConsumptionTargetCalculator(targetQueueSettingsProvider, capacityCalculatorBase,
                        new StagingQueueWeightSettingsProvider());

        final Map<Queue,Long> consumptionTargets =
                fastLaneConsumptionTargetCalculator.calculateConsumptionTargets(distributorWorkItem);
        final long queue1Result = consumptionTargets.get(q1);
        final long queue2Result = consumptionTargets.get(q2);

        assertEquals("Queue 1 gets capacity to fill half of the available targetQueueCapacity",
                500, queue1Result);
        assertEquals("Queue 1 gets capacity to fill half of the available targetQueueCapacity",
                500, queue2Result);
        assertEquals("Both queues are larger than half of the target queue capacity therefore " +
                "each are given half of the available target queue capacity, regardless of the fact that " +
                "staging queue 2 is much larger than staging queue 1.",
                targetQueueCapacity, queue1Result+queue2Result);
    }

    @Test
    public void calculateCapacityAvailableForMultipleStagingQueueTest() {

        // Capacity of 1000 is available. This means 100 messages per queue.
        // q2, q3, q5, q6, q9 do not all need this.
        // This will be redistributed to the larger queues which all have enough
        // messages to use that extra capacity. this means there is no re-calculation required.

        final Queue targetQueue = getQueue("tq", 1000);

        final Queue q1 = getQueue("sq1", 1000);
        final Queue q2 = getQueue("sq2", 50);
        final Queue q3 = getQueue("sq3", 60);
        final Queue q4 = getQueue("sq4", 200);
        final Queue q5 = getQueue("sq5", 30);
        final Queue q6 = getQueue("sq6", 50);
        final Queue q7 = getQueue("sq7", 9000);
        final Queue q8 = getQueue("sq8", 700);
        final Queue q9 = getQueue("sq9", 10);
        final Queue q10 = getQueue("sq10", 500);

        final Set<Queue> stagingQueues =
                new HashSet<>(Arrays.asList(q1, q2, q3, q4, q5, q6, q7, q8, q9, q10));
        final DistributorWorkItem distributorWorkItem = mock(DistributorWorkItem.class);
        when(distributorWorkItem.getStagingQueues()).thenReturn(stagingQueues);
        when(distributorWorkItem.getTargetQueue()).thenReturn(targetQueue);

        final TargetQueueSettings targetQueueSettings =
                new TargetQueueSettings(1000,10, 1, 1, 1000L);

        final TargetQueueSettingsProvider targetQueueSettingsProvider = mock(TargetQueueSettingsProvider.class);
        when(targetQueueSettingsProvider.get(targetQueue)).thenReturn(targetQueueSettings);

        final long targetQueueCapacity = targetQueueSettings.getCapacity();

        final CapacityCalculatorBase capacityCalculatorBase = mock(CapacityCalculatorBase.class);
        when(capacityCalculatorBase.refine(any(), any())).thenReturn(targetQueueSettings);

        final FastLaneConsumptionTargetCalculator fastLaneConsumptionTargetCalculator =
                new FastLaneConsumptionTargetCalculator(targetQueueSettingsProvider, capacityCalculatorBase,
                        new StagingQueueWeightSettingsProvider());

        final Map<Queue,Long> consumptionTargets =
                fastLaneConsumptionTargetCalculator.calculateConsumptionTargets(distributorWorkItem);
        final long queue1Result = consumptionTargets.get(q1);
        final long queue2Result = consumptionTargets.get(q2);
        final long queue3Result = consumptionTargets.get(q3);
        final long queue4Result = consumptionTargets.get(q4);
        final long queue5Result = consumptionTargets.get(q5);
        final long queue6Result = consumptionTargets.get(q6);
        final long queue7Result = consumptionTargets.get(q7);
        final long queue8Result = consumptionTargets.get(q8);
        final long queue9Result = consumptionTargets.get(q9);
        final long queue10Result = consumptionTargets.get(q10);

        final long queueConsumptionTargetSum =
                queue1Result + queue2Result + queue3Result + queue4Result + queue5Result +
                queue6Result + queue7Result + queue8Result + queue9Result + queue10Result;

        final boolean q1ConsumptionRateIncrease = queue1Result > (targetQueueCapacity / stagingQueues.size());

        assertEquals("Sum of each staging queue consumption target is the total capacity " +
                "available on the target queue",
                targetQueueCapacity, queueConsumptionTargetSum);
        assertTrue("Consumption rate of queue 1 should be greater than the original equal " +
                "consumption, as multiple queues are smaller than the targetQueueCapacity / num of staging queues.",
                q1ConsumptionRateIncrease);
    }

    @Test
    public void calculateCapacityAvailableForMultipleStagingQueueNeedingRecalculatedAfterFirstWeightingTest() {

        // The sum of capacity taken by each staging queue should equal the capacity of 1000.
        // In this case initially the q2 and q3 will be given a share of the 50 messages left over from q4.
        // However, this will be re-calculated as this capacity is not all needed by q2,
        // and none of this extra capacity is needed by q3.
        // The leftover capacity from q2 and q3 will be distributed to q1.
        // This will mean the total capacity used by the 4 queues is the full available capacity of 1000.

        final Queue targetQueue = getQueue("tq", 1000);

        final Queue q1 = getQueue("sq1", 1000);
        final Queue q2 = getQueue("sq2", 250);
        final Queue q3 = getQueue("sq3", 260);
        final Queue q4 = getQueue("sq4", 200);

        final Set<Queue> stagingQueues = new HashSet<>(Arrays.asList(q1, q2, q3, q4));
        final DistributorWorkItem distributorWorkItem = mock(DistributorWorkItem.class);
        when(distributorWorkItem.getStagingQueues()).thenReturn(stagingQueues);
        when(distributorWorkItem.getTargetQueue()).thenReturn(targetQueue);

        final TargetQueueSettings targetQueueSettings =
                new TargetQueueSettings(1000,10, 1, 1, 1000L);

        final TargetQueueSettingsProvider targetQueueSettingsProvider = mock(TargetQueueSettingsProvider.class);
        when(targetQueueSettingsProvider.get(targetQueue)).thenReturn(targetQueueSettings);

        final long targetQueueCapacity = targetQueueSettings.getCapacity();

        final CapacityCalculatorBase capacityCalculatorBase = mock(CapacityCalculatorBase.class);
        when(capacityCalculatorBase.refine(any(), any())).thenReturn(targetQueueSettings);

        final FastLaneConsumptionTargetCalculator fastLaneConsumptionTargetCalculator =
                new FastLaneConsumptionTargetCalculator(targetQueueSettingsProvider,capacityCalculatorBase,
                        new StagingQueueWeightSettingsProvider());

        final Map<Queue,Long> consumptionTargets = fastLaneConsumptionTargetCalculator
                .calculateConsumptionTargets(distributorWorkItem);
        final long queue1Result = consumptionTargets.get(q1);
        final long queue2Result = consumptionTargets.get(q2);
        final long queue3Result = consumptionTargets.get(q3);
        final long queue4Result = consumptionTargets.get(q4);

        final long queueConsumptionTargetSum =
                queue1Result + queue2Result + queue3Result + queue4Result;
        
        assertEquals("Sum of each staging queue consumption target is the total capacity " +
                        "available on the target queue",
                targetQueueCapacity, queueConsumptionTargetSum, 1.0);
    }

    @Test
    public void calculateCapacityAvailableForMultipleQueuesWhichAreSmallerThanAvailableCapacityTest() {

        // 10000 capacity available however the staging queues have fewer messages combined than that.
        // Ensure that in this case each staging queue is given capacity for its entire queue

        final Queue targetQueue = getQueue("tq", 1000);

        final Queue q1 = getQueue("sq1", 3500);
        final Queue q2 = getQueue("sq2", 25);
        final Queue q3 = getQueue("sq3", 2);

        final Set<Queue> stagingQueues = new HashSet<>(Arrays.asList(q1, q2, q3));
        final DistributorWorkItem distributorWorkItem = mock(DistributorWorkItem.class);
        when(distributorWorkItem.getStagingQueues()).thenReturn(stagingQueues);
        when(distributorWorkItem.getTargetQueue()).thenReturn(targetQueue);

        final TargetQueueSettings targetQueueSettings =
                new TargetQueueSettings(1000,10, 1, 1, 10000L);

        final TargetQueueSettingsProvider targetQueueSettingsProvider = mock(TargetQueueSettingsProvider.class);
        when(targetQueueSettingsProvider.get(targetQueue)).thenReturn(targetQueueSettings);

        final CapacityCalculatorBase capacityCalculatorBase = mock(CapacityCalculatorBase.class);
        when(capacityCalculatorBase.refine(any(), any())).thenReturn(targetQueueSettings);

        final FastLaneConsumptionTargetCalculator fastLaneConsumptionTargetCalculator =
                new FastLaneConsumptionTargetCalculator(targetQueueSettingsProvider, capacityCalculatorBase,
                        new StagingQueueWeightSettingsProvider());

        final Map<Queue,Long> consumptionTargets = fastLaneConsumptionTargetCalculator
                .calculateConsumptionTargets(distributorWorkItem);
        final long queue1Result = consumptionTargets.get(q1);
        final long queue2Result = consumptionTargets.get(q2);
        final long queue3Result = consumptionTargets.get(q3);

        final long queueConsumptionTargetSum =
                queue1Result + queue2Result + queue3Result;

        final long totalQueueMessages =
                q1.getMessages() + q2.getMessages() + q3.getMessages();

        assertEquals("In the case that the sum of the staging queues does not fill the capacity of target" +
                    "queue available. The staging queue should be given capacity equal to its length. In other " +
                    "words the queueConsumptionTargetSum should be equal to the total of all messages on the staging " +
                    "queues regardless of queue size.",
                totalQueueMessages, queueConsumptionTargetSum, 0.0);
    }

    @Test
    public void calculateCapacityAvailableForMultipleQueuesWithWeightedRegexTest() {

        // Ensure the message capacity given to each queue correlates to the given weight

        final Queue targetQueue = getQueue("tq", 1000);

        final Queue q1 = getQueue("bulk-indexer-in»/clynch/enrichment-workflow", 3500);
        final Queue q2 = getQueue("dataprocessing-classification-in»/clynch/update-entities-workflow", 2500);
        final Queue q3 = getQueue("sq3", 4000);

        final Set<Queue> stagingQueues = new HashSet<>(Arrays.asList(q1, q2, q3));
        final DistributorWorkItem distributorWorkItem = mock(DistributorWorkItem.class);
        when(distributorWorkItem.getStagingQueues()).thenReturn(stagingQueues);
        when(distributorWorkItem.getTargetQueue()).thenReturn(targetQueue);

        final TargetQueueSettings targetQueueSettings =
                new TargetQueueSettings(1000,10, 1, 1, 140L);

        final TargetQueueSettingsProvider targetQueueSettingsProvider = mock(TargetQueueSettingsProvider.class);
        when(targetQueueSettingsProvider.get(targetQueue)).thenReturn(targetQueueSettings);

        final CapacityCalculatorBase capacityCalculatorBase = mock(CapacityCalculatorBase.class);
        when(capacityCalculatorBase.refine(any(), any())).thenReturn(targetQueueSettings);

        final long targetQueueCapacity = targetQueueSettings.getCapacity();

        // This map represents 2 regex added to environment variables giving enrichment-workflow matches a weight of 10, and clynch
        // tenant matches a weight of 0. Enrichment-workflow is a larger regex match, so it will override the clynch regex.
        final Map<String, Double> stagingQueueWeightMap = new HashMap<>();
        stagingQueueWeightMap.put("bulk-indexer-in»/clynch/enrichment-workflow", 10D);
        stagingQueueWeightMap.put("dataprocessing-classification-in»/clynch/update-entities-workflow", 0D);
        stagingQueueWeightMap.put("sq3", 1D);

        final StagingQueueWeightSettingsProvider stagingQueueWeightSettingsProvider =
                mock(StagingQueueWeightSettingsProvider.class);
        when(stagingQueueWeightSettingsProvider.getStagingQueueWeights(anyList())).thenReturn(stagingQueueWeightMap);

        final FastLaneConsumptionTargetCalculator fastLaneConsumptionTargetCalculator =
                new FastLaneConsumptionTargetCalculator
                        (targetQueueSettingsProvider, capacityCalculatorBase, stagingQueueWeightSettingsProvider);

        final Map<Queue,Long> consumptionTargets = fastLaneConsumptionTargetCalculator
                .calculateConsumptionTargets(distributorWorkItem);
        final long queue1Result = consumptionTargets.get(q1);
        final long queue2Result = consumptionTargets.get(q2);
        final long queue3Result = consumptionTargets.get(q3);

        final long queueConsumptionTargetSum =
                queue1Result + queue2Result + queue3Result;

        final long weightedValueExpected = queue3Result * 10;

        assertEquals("The total consumption of each queue should add up to the total target queue capacity available.",
                targetQueueCapacity, queueConsumptionTargetSum, 2.0);

        assertEquals("Queue 1 has been weighted 10 and sq3 weighted 1. " +
                        "This means queue 1 should be given 10 times the message capacity of queue 3.",
                weightedValueExpected, queue1Result, 2.0);

        assertEquals("Queue 2 has been weighted 0. This means it should be given no target queue capacity",
                0, queue2Result, 0.0);
    }

    Queue getQueue(final String name, final long messages) {
        final Queue queue = new Queue();
        queue.setName(name);
        queue.setMessages(messages);
        return queue;
    }
}
