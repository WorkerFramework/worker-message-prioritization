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
import com.github.workerframework.workermessageprioritization.targetqueue.TargetQueuePerformanceMetricsProvider;
import com.github.workerframework.workermessageprioritization.targetqueue.HistoricalConsumptionRate;
import com.github.workerframework.workermessageprioritization.targetqueue.RoundTargetQueueLength;
import com.github.workerframework.workermessageprioritization.targetqueue.TunedTargetQueueLengthProvider;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mockito.Answers.CALLS_REAL_METHODS;
import static org.mockito.Answers.RETURNS_DEFAULTS;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.withSettings;
import static org.mockito.Mockito.when;

public final class MinimumConsumptionTargetCalculatorTest
{

    /**
     * Target Queue Max Length: 1000 Target Queue Available Capacity: 750 (25%) Target Queue Eligible For Refill Percentage: 20%
     *
     * Therefore, messages should be moved - expect 250 to be returned
     */
    @Test
    public void getTargetQueueCapacityTest()
    {
        final TargetQueueSettingsProvider targetQueueSettingsProvider = mock(TargetQueueSettingsProvider.class);
        final Queue targetQueue = new Queue();
        targetQueue.setMessages(750);

        final TargetQueueSettings settings = new TargetQueueSettings(1000, 20, 1, 1);
        when(targetQueueSettingsProvider.get(targetQueue)).thenReturn(settings);

        final double consumptionRate = 0.5;
        final TargetQueuePerformanceMetricsProvider targetQueuePerformanceMetricsProvider = mock(TargetQueuePerformanceMetricsProvider.class);
        when(targetQueuePerformanceMetricsProvider.getTargetQueuePerformanceMetrics(anyString())).thenReturn(consumptionRate);

        final HistoricalConsumptionRate historicalConsumptionRate = mock(HistoricalConsumptionRate.class);
        when(historicalConsumptionRate.recordCurrentConsumptionRateHistoryAndGetAverage(anyString(), anyDouble())).thenReturn(consumptionRate);
        when(historicalConsumptionRate.isSufficientHistoryAvailable(anyString())).thenReturn(false);

        final int roundingMultiple = 100;
        final RoundTargetQueueLength roundTargetQueueLength= mock(RoundTargetQueueLength.class,
                withSettings().useConstructor(roundingMultiple).defaultAnswer(RETURNS_DEFAULTS));
        when(roundTargetQueueLength.getRoundedTargetQueueLength(anyLong())).thenReturn(1000L);

        final boolean noOpMode = true;
        final double queueProcessingTimeGoalSeconds = 300D;
        final TunedTargetQueueLengthProvider tunedTargetQueueLengthProvider = mock(TunedTargetQueueLengthProvider.class,
                withSettings().useConstructor(targetQueuePerformanceMetricsProvider, historicalConsumptionRate,
                        roundTargetQueueLength, noOpMode, queueProcessingTimeGoalSeconds).defaultAnswer(RETURNS_DEFAULTS));

        final MinimumConsumptionTargetCalculator minimumConsumptionTargetCalculator = mock(
            MinimumConsumptionTargetCalculator.class,
            withSettings().useConstructor(targetQueueSettingsProvider, tunedTargetQueueLengthProvider).defaultAnswer(CALLS_REAL_METHODS));

        assertEquals(250, minimumConsumptionTargetCalculator.getTargetQueueCapacity(targetQueue));
    }

    /**
     * Target Queue Max Length: 1000 Target Queue Available Capacity: 750 (25%) Target Queue Eligible For Refill Percentage: 30%
     *
     * Therefore, no messages should be moved - expect 0 to be returned
     */
    @Test
    public void getTargetQueueCapacityTestReturn0()
    {
        final TargetQueueSettingsProvider targetQueueSettingsProvider = mock(TargetQueueSettingsProvider.class);
        final Queue targetQueue = new Queue();
        targetQueue.setMessages(750);

        final TargetQueueSettings settings = new TargetQueueSettings(1000, 30, 1 , 1);
        when(targetQueueSettingsProvider.get(targetQueue)).thenReturn(settings);

        final double consumptionRate = 0.5;
        final TargetQueuePerformanceMetricsProvider targetQueuePerformanceMetricsProvider = mock(TargetQueuePerformanceMetricsProvider.class);
        when(targetQueuePerformanceMetricsProvider.getTargetQueuePerformanceMetrics(anyString())).thenReturn(consumptionRate);

        final HistoricalConsumptionRate historicalConsumptionRate = mock(HistoricalConsumptionRate.class);
        when(historicalConsumptionRate.recordCurrentConsumptionRateHistoryAndGetAverage(anyString(), anyDouble())).thenReturn(consumptionRate);
        when(historicalConsumptionRate.isSufficientHistoryAvailable(anyString())).thenReturn(false);

        final int roundingMultiple = 100;
        final RoundTargetQueueLength roundTargetQueueLength= mock(RoundTargetQueueLength.class,
                withSettings().useConstructor(roundingMultiple).defaultAnswer(RETURNS_DEFAULTS));
        when(roundTargetQueueLength.getRoundedTargetQueueLength(anyLong())).thenReturn(1000L);

        final boolean noOpMode = true;
        final double queueProcessingTimeGoalSeconds = 300D;
        final TunedTargetQueueLengthProvider tunedTargetQueueLengthProvider = mock(TunedTargetQueueLengthProvider.class,
                withSettings().useConstructor(targetQueuePerformanceMetricsProvider, historicalConsumptionRate,
                        roundTargetQueueLength, noOpMode, queueProcessingTimeGoalSeconds).defaultAnswer(RETURNS_DEFAULTS));

        final MinimumConsumptionTargetCalculator minimumConsumptionTargetCalculator = mock(
            MinimumConsumptionTargetCalculator.class,
            withSettings().useConstructor(targetQueueSettingsProvider, tunedTargetQueueLengthProvider).defaultAnswer(CALLS_REAL_METHODS));

        assertEquals(0, minimumConsumptionTargetCalculator.getTargetQueueCapacity(targetQueue));
    }
}
