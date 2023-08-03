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
import com.github.workerframework.workermessageprioritization.targetqueue.TargetQueueSettingsProvider;
import com.github.workerframework.workermessageprioritization.targetqueue.TargetQueueSettings;
import com.github.workerframework.workermessageprioritization.targetqueue.QueueConsumptionRateProvider;
import com.github.workerframework.workermessageprioritization.targetqueue.HistoricalConsumptionRateManager;
import com.github.workerframework.workermessageprioritization.targetqueue.TargetQueueLengthRounder;
import com.github.workerframework.workermessageprioritization.targetqueue.TunedTargetQueueLengthProvider;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mockito.Answers.CALLS_REAL_METHODS;
import static org.mockito.Answers.RETURNS_DEFAULTS;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

public final class ConsumptionTargetCalculatorBaseTest
{

    @Test
    public void getTargetQueueCapacityRegardlessOfRefillPercentageTest()
    {
        final TargetQueueSettingsProvider targetQueueSettingsProvider = mock(TargetQueueSettingsProvider.class);
        final Queue targetQueue = new Queue();
        targetQueue.setMessages(1000);

        final double consumptionRate = 0.5;
        final QueueConsumptionRateProvider queueConsumptionRateProvider = mock(QueueConsumptionRateProvider.class);
        when(queueConsumptionRateProvider.getConsumptionRate(anyString())).thenReturn(consumptionRate);

        final HistoricalConsumptionRateManager historicalConsumptionRateManager = mock(HistoricalConsumptionRateManager.class);
        when(historicalConsumptionRateManager.recordCurrentConsumptionRateHistoryAndGetAverage(anyString(), anyDouble())).thenReturn(consumptionRate);
        when(historicalConsumptionRateManager.isSufficientHistoryAvailable(anyString())).thenReturn(false);

        final int roundingMultiple = 100;
        final TargetQueueLengthRounder targetQueueLengthRounder = mock(TargetQueueLengthRounder.class,
                withSettings().useConstructor(roundingMultiple).defaultAnswer(RETURNS_DEFAULTS));
        when(targetQueueLengthRounder.getRoundedTargetQueueLength(anyLong())).thenReturn(1000L);

        final boolean noOpMode = true;
        final double queueProcessingTimeGoalSeconds = 300D;
        final TunedTargetQueueLengthProvider tunedTargetQueueLengthProvider = mock(TunedTargetQueueLengthProvider.class,
                withSettings().useConstructor(queueConsumptionRateProvider, historicalConsumptionRateManager,
                        targetQueueLengthRounder, noOpMode, queueProcessingTimeGoalSeconds).defaultAnswer(RETURNS_DEFAULTS));

        final TargetQueueSettings settings = new TargetQueueSettings(1200, 50, 1, 1);
        when(targetQueueSettingsProvider.get(targetQueue)).thenReturn(settings);

        final ConsumptionTargetCalculatorBase calculator = mock(
                ConsumptionTargetCalculatorBase.class,
                withSettings().useConstructor(targetQueueSettingsProvider, tunedTargetQueueLengthProvider).defaultAnswer(CALLS_REAL_METHODS));

        assertEquals("Message capacity available returned regardless of the eligible for refill percentage set.", 200,
                calculator.getTargetQueueCapacity(targetQueue, 100,
                10000000));
    }

    @Test
    public void getTargetQueueCapacityTestMessageCountAboveLimitReturn0()
    {
        final TargetQueueSettingsProvider targetQueueSettingsProvider = mock(TargetQueueSettingsProvider.class);
        final Queue targetQueue = new Queue();
        targetQueue.setMessages(1200);

        final double consumptionRate = 0.5;
        final QueueConsumptionRateProvider queueConsumptionRateProvider = mock(QueueConsumptionRateProvider.class);
        when(queueConsumptionRateProvider.getConsumptionRate(anyString())).thenReturn(consumptionRate);

        final HistoricalConsumptionRateManager historicalConsumptionRateManager = mock(HistoricalConsumptionRateManager.class);
        when(historicalConsumptionRateManager.recordCurrentConsumptionRateHistoryAndGetAverage(anyString(), anyDouble())).thenReturn(consumptionRate);
        when(historicalConsumptionRateManager.isSufficientHistoryAvailable(anyString())).thenReturn(false);

        final int roundingMultiple = 100;
        final TargetQueueLengthRounder targetQueueLengthRounder = mock(TargetQueueLengthRounder.class,
                withSettings().useConstructor(roundingMultiple).defaultAnswer(RETURNS_DEFAULTS));
        when(targetQueueLengthRounder.getRoundedTargetQueueLength(anyLong())).thenReturn(1000L);

        final boolean noOpMode = true;
        final double queueProcessingTimeGoalSeconds = 300D;
        final TunedTargetQueueLengthProvider tunedTargetQueueLengthProvider = mock(TunedTargetQueueLengthProvider.class,
                withSettings().useConstructor(queueConsumptionRateProvider, historicalConsumptionRateManager,
                        targetQueueLengthRounder, noOpMode, queueProcessingTimeGoalSeconds).defaultAnswer(RETURNS_DEFAULTS));

        final TargetQueueSettings settings = new TargetQueueSettings(1000, 20, 1, 1);
        when(targetQueueSettingsProvider.get(targetQueue)).thenReturn(settings);

        final ConsumptionTargetCalculatorBase calculator = mock(
            ConsumptionTargetCalculatorBase.class,
            withSettings().useConstructor(targetQueueSettingsProvider, tunedTargetQueueLengthProvider).defaultAnswer(CALLS_REAL_METHODS));

        assertEquals("There are more messages on the queue than the set maximum target queue length available therefore no space " +
                        "available and 0 returned.", 0,
                calculator.getTargetQueueCapacity(targetQueue, 100, 10000000));
    }
}
