package com.github.workerframework.workermessageprioritization.redistribution.consumption;

import com.github.workerframework.workermessageprioritization.rabbitmq.Queue;
import com.github.workerframework.workermessageprioritization.redistribution.DistributorWorkItem;
import com.github.workerframework.workermessageprioritization.targetqueue.TargetQueueSettings;
import com.github.workerframework.workermessageprioritization.targetqueue.TargetQueueSettingsProvider;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ErrorCollector;

import java.util.HashSet;
import java.util.Arrays;
import java.util.Set;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public final class FastLaneConsumptionTargetCalculatorTest {

    @Rule
    public final ErrorCollector collector = new ErrorCollector();

    @Test
    public void calculateConsumptionTargetFor1LargeAnd1SmallStagingQueueTest() {

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

        final int targetQueueMessages = 1000;
        final String targetQueueName = "tq";
        final int q1Messages = 1000;
        final int q2Messages = 50;
        final String q1Name = "q1";
        final String q2Name = "q2";

        final Queue targetQueue = mock(Queue.class);
        targetQueue.setMessages(targetQueueMessages);
        targetQueue.setName(targetQueueName);
        final Queue q1 = new Queue();
        final Queue q2 = new Queue();
        q1.setMessages(q1Messages);
        q2.setMessages(q2Messages);
        q1.setName(q1Name);
        q1.setName(q2Name);
        final Set<Queue> stagingQueues = new HashSet<>(Arrays.asList(q1, q2));
        final DistributorWorkItem distributorWorkItem = mock(DistributorWorkItem.class);
        when(distributorWorkItem.getStagingQueues()).thenReturn(stagingQueues);
        when(distributorWorkItem.getTargetQueue()).thenReturn(targetQueue);

        final TargetQueueSettings targetQueueSettings = new TargetQueueSettings(1000,10);

        final TargetQueueSettingsProvider targetQueueSettingsProvider = mock(TargetQueueSettingsProvider.class);
        when(targetQueueSettingsProvider.get(targetQueue)).thenReturn(targetQueueSettings);

        final long targetQueueCapacity = 1000L;

        final FastLaneConsumptionTargetCalculator fastLaneConsumptionTargetCalculator =
                new FastLaneConsumptionTargetCalculator(targetQueueSettingsProvider);

        final Map<Queue,Long> consumptionTargets = fastLaneConsumptionTargetCalculator.calculateConsumptionTargets(distributorWorkItem);
        final int queue1Result = Math.toIntExact(consumptionTargets.get(q1));
        final int queue2Result = Math.toIntExact(consumptionTargets.get(q2));

        assertEquals("Queue 1 has more than 500 messages, therefore it should be offered the rest of the capacity that queue 2 does not" +
                " need", 950, queue1Result);
        assertEquals("Queue 2 only has less than 500 messages therefore the capacity of the target queue it will fill is the size of " +
                        "it's staging queue.",
                50, queue2Result);
        assertEquals("Confirm the total target queue capacity has been used", targetQueueCapacity, queue1Result+queue2Result);
    }

    @Test
    public void calculateConsumptionTargetFor2LargeStagingQueuesTest() {

        final int targetQueueMessages = 1000;
        final String targetQueueName = "tq";
        final int q1Messages = 1000;
        final int q2Messages = 10000;
        final String q1Name = "q1";
        final String q2Name = "q2";

        final Queue targetQueue = mock(Queue.class);
        targetQueue.setMessages(targetQueueMessages);
        targetQueue.setName(targetQueueName);
        final Queue q1 = new Queue();
        final Queue q2 = new Queue();
        q1.setMessages(q1Messages);
        q2.setMessages(q2Messages);
        q1.setName(q1Name);
        q1.setName(q2Name);
        final Set<Queue> stagingQueues = new HashSet<>(Arrays.asList(q1, q2));
        final DistributorWorkItem distributorWorkItem = mock(DistributorWorkItem.class);
        when(distributorWorkItem.getStagingQueues()).thenReturn(stagingQueues);
        when(distributorWorkItem.getTargetQueue()).thenReturn(targetQueue);

        final TargetQueueSettings targetQueueSettings = new TargetQueueSettings(1000,10);

        final TargetQueueSettingsProvider targetQueueSettingsProvider = mock(TargetQueueSettingsProvider.class);
        when(targetQueueSettingsProvider.get(targetQueue)).thenReturn(targetQueueSettings);

        final long targetQueueCapacity = 1000L;

        final FastLaneConsumptionTargetCalculator fastLaneConsumptionTargetCalculator =
                new FastLaneConsumptionTargetCalculator(targetQueueSettingsProvider);

        final Map<Queue,Long> consumptionTargets = fastLaneConsumptionTargetCalculator.calculateConsumptionTargets(distributorWorkItem);
        final int queue1Result = Math.toIntExact(consumptionTargets.get(q1));
        final int queue2Result = Math.toIntExact(consumptionTargets.get(q2));

        assertEquals("Queue 1 gets capacity to fill half of the available targetQueueCapacity", 500, queue1Result);
        assertEquals("Queue 1 gets capacity to fill half of the available targetQueueCapacity",
                500, queue2Result);
        assertEquals("Both queues are larger than half of the target queue capacity therefore each are given half of the available " +
                        "targetqueue capacity, regardless of the fact that staging queue 2 is much larger than staging queue 1.",
                targetQueueCapacity, queue1Result+queue2Result);
    }


    @Test
    public void calculateConsumptionTargetForMultipleStagingQueueTest() {

        final int targetQueueMessages = 1000;
        final String targetQueueName = "tq";
        final int q1Messages = 1000;
        final int q2Messages = 50;
        final int q3Messages = 60;
        final int q4Messages = 200;
        final int q5Messages = 30;
        final int q6Messages = 50;
        final int q7Messages = 9000;
        final int q8Messages = 700;
        final int q9Messages = 10;
        final int q10Messages = 500;

        final Queue targetQueue = mock(Queue.class);
        targetQueue.setMessages(targetQueueMessages);
        targetQueue.setName(targetQueueName);
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
        q1.setMessages(q1Messages);
        q2.setMessages(q2Messages);
        q3.setMessages(q3Messages);
        q4.setMessages(q4Messages);
        q5.setMessages(q5Messages);
        q6.setMessages(q6Messages);
        q7.setMessages(q7Messages);
        q8.setMessages(q8Messages);
        q9.setMessages(q9Messages);
        q10.setMessages(q10Messages);
        final Set<Queue> stagingQueues = new HashSet<>(Arrays.asList(q1, q2, q3, q4, q5, q6, q7, q8, q9, q10));
        final DistributorWorkItem distributorWorkItem = mock(DistributorWorkItem.class);
        when(distributorWorkItem.getStagingQueues()).thenReturn(stagingQueues);
        when(distributorWorkItem.getTargetQueue()).thenReturn(targetQueue);

        final TargetQueueSettings targetQueueSettings = new TargetQueueSettings(1000,10);

        final TargetQueueSettingsProvider targetQueueSettingsProvider = mock(TargetQueueSettingsProvider.class);
        when(targetQueueSettingsProvider.get(targetQueue)).thenReturn(targetQueueSettings);

        final long targetQueueCapacity = 1000L;

        final FastLaneConsumptionTargetCalculator fastLaneConsumptionTargetCalculator =
                new FastLaneConsumptionTargetCalculator(targetQueueSettingsProvider);

        final Map<Queue,Long> consumptionTargets = fastLaneConsumptionTargetCalculator.calculateConsumptionTargets(distributorWorkItem);
        final int queue1Result = Math.toIntExact(consumptionTargets.get(q1));
        final int queue2Result = Math.toIntExact(consumptionTargets.get(q2));
        final int queue3Result = Math.toIntExact(consumptionTargets.get(q3));
        final int queue4Result = Math.toIntExact(consumptionTargets.get(q4));
        final int queue5Result = Math.toIntExact(consumptionTargets.get(q5));
        final int queue6Result = Math.toIntExact(consumptionTargets.get(q6));
        final int queue7Result = Math.toIntExact(consumptionTargets.get(q7));
        final int queue8Result = Math.toIntExact(consumptionTargets.get(q8));
        final int queue9Result = Math.toIntExact(consumptionTargets.get(q9));
        final int queue10Result = Math.toIntExact(consumptionTargets.get(q10));

        final int queueConsumptionTargetSum =
                queue1Result + queue2Result + queue3Result + queue4Result + queue5Result + queue6Result + queue7Result + queue8Result + queue9Result + queue10Result;

        final boolean q1ConsumptionRateIncrease = queue1Result > (targetQueueCapacity / stagingQueues.size());

        assertEquals("Sum of each staging queue consumption target is the total capacity available on the target queue", targetQueueCapacity,
                queueConsumptionTargetSum);
        assertEquals("Consumption rate of queue 1 should be greater than the original equal consumption, as multiple queues are smaller" +
                        " than the targetQueueCapacity / num of staging queues.", true, q1ConsumptionRateIncrease);
    }

}
