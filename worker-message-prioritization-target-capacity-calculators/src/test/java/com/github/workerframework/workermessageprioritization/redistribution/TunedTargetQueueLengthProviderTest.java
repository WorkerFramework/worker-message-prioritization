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

import com.github.workerframework.workermessageprioritization.targetqueue.TunedTargetQueueLengthProvider;
import com.github.workerframework.workermessageprioritization.targetqueue.HistoricalConsumptionRate;
import com.github.workerframework.workermessageprioritization.targetqueue.TargetQueuePerformanceMetricsProvider;
import com.github.workerframework.workermessageprioritization.targetqueue.RoundTargetQueueLength;
import com.github.workerframework.workermessageprioritization.targetqueue.TargetQueueSettings;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;
import static org.mockito.Answers.RETURNS_DEFAULTS;

import org.mockito.ArgumentMatchers;

public class TunedTargetQueueLengthProviderTest {

    private final long targetQueueLength = 10000;
    private final String targetQueue1 = "targetQueue1";
    private final String targetQueue2 = "targetQueue2";
    private final long minTargetQueueLength = 100;
    private final long maxTargetQueueLength = 1000000;
    private final long queueProcessingTimeGoalSeconds = 300; //5 minutes
    private final double currentInstances = 2;
    private final double maxInstances = 4;
    private final int roundingMultiple = 100;
    private final int eligableForRefillPercentage = 10;

    @Test
    public void getTunedTargetQueueNoOpTest(){

        final double consumptionRate = 0.5;

        final TargetQueueSettings targetQueueSettings = new TargetQueueSettings(targetQueueLength, eligableForRefillPercentage,
                maxInstances, currentInstances);

        final HistoricalConsumptionRate historicalConsumptionRate = mock(HistoricalConsumptionRate.class);
        when(historicalConsumptionRate.isSufficientHistoryAvailable(ArgumentMatchers.anyString())).thenReturn(false);
        when(historicalConsumptionRate.recordCurrentConsumptionRateHistoryAndGetAverage(ArgumentMatchers.anyString(),
                ArgumentMatchers.anyDouble())).thenReturn(1.0);

        final RoundTargetQueueLength roundTargetQueueLength = mock(RoundTargetQueueLength.class,
                withSettings().useConstructor(roundingMultiple).defaultAnswer(RETURNS_DEFAULTS));
        when(roundTargetQueueLength.getRoundedTargetQueueLength(ArgumentMatchers.anyLong())).thenReturn(300L);

        final TargetQueuePerformanceMetricsProvider targetQueuePerformanceMetricsProvider =
                mock(TargetQueuePerformanceMetricsProvider.class);
        when(targetQueuePerformanceMetricsProvider.getTargetQueuePerformanceMetrics(targetQueue1))
                .thenReturn(consumptionRate);

        final TunedTargetQueueLengthProvider targetQueue = new TunedTargetQueueLengthProvider(targetQueuePerformanceMetricsProvider,
                historicalConsumptionRate, roundTargetQueueLength, true, queueProcessingTimeGoalSeconds);

        final long tunedTargetQueueLength = getTunedTargetQueueLength(targetQueue1,targetQueue, targetQueueSettings);

        assertEquals("NoOpMode is on: Target queue length should not have changed. Suggested adjustment for the target queue length " +
                "should still be logged.", targetQueueLength, tunedTargetQueueLength);
    }

    @Test
    public void getTunedTargetQueueForQueueWithInadequateHistoryAndNoOpModeOffTest(){

        final double consumptionRate = 0.5;

        final TargetQueueSettings targetQueueSettings = new TargetQueueSettings(targetQueueLength, eligableForRefillPercentage,
                maxInstances, currentInstances);

        final HistoricalConsumptionRate historicalConsumptionRate = mock(HistoricalConsumptionRate.class);
        when(historicalConsumptionRate.isSufficientHistoryAvailable(ArgumentMatchers.anyString())).thenReturn(false);
        when(historicalConsumptionRate.recordCurrentConsumptionRateHistoryAndGetAverage(ArgumentMatchers.anyString(),
                ArgumentMatchers.anyDouble())).thenReturn(1.0);

        final RoundTargetQueueLength roundTargetQueueLength = mock(RoundTargetQueueLength.class,
                withSettings().useConstructor(roundingMultiple).defaultAnswer(RETURNS_DEFAULTS));
        when(roundTargetQueueLength.getRoundedTargetQueueLength(ArgumentMatchers.anyLong())).thenReturn(300L);

        final TargetQueuePerformanceMetricsProvider targetQueuePerformanceMetricsProvider =
                mock(TargetQueuePerformanceMetricsProvider.class);
        when(targetQueuePerformanceMetricsProvider.getTargetQueuePerformanceMetrics(targetQueue1))
                .thenReturn(consumptionRate);

        final TunedTargetQueueLengthProvider targetQueue = new TunedTargetQueueLengthProvider(targetQueuePerformanceMetricsProvider,
                historicalConsumptionRate, roundTargetQueueLength, false, queueProcessingTimeGoalSeconds);

        final long tunedTargetQueueLength = getTunedTargetQueueLength(targetQueue1, targetQueue, targetQueueSettings);

        assertEquals("isSufficientHistoryAvailable set to false: Not enough consumption rate history. Target queue length should not " +
                        "change.", targetQueueLength, tunedTargetQueueLength);
    }


    @Test
    public void getDifferentTunedTargetQueueLengthsForQueuesWithAdequateHistoryAndNoOpModeOffTest(){

        final double consumptionRate1 = 0.5;
        final double consumptionRate2 = 5;

        final TargetQueueSettings targetQueueSettings = new TargetQueueSettings(targetQueueLength, eligableForRefillPercentage,
                maxInstances, currentInstances);

        final HistoricalConsumptionRate historicalConsumptionRate = mock(HistoricalConsumptionRate.class);
        when(historicalConsumptionRate.isSufficientHistoryAvailable(targetQueue1)).thenReturn(true);
        when(historicalConsumptionRate.isSufficientHistoryAvailable(targetQueue2)).thenReturn(true);
        when(historicalConsumptionRate.recordCurrentConsumptionRateHistoryAndGetAverage(targetQueue1,
                1.0)).thenReturn(1.0);
        when(historicalConsumptionRate.recordCurrentConsumptionRateHistoryAndGetAverage(targetQueue2,
                10.0)).thenReturn(10.0);

        final RoundTargetQueueLength roundTargetQueueLength = mock(RoundTargetQueueLength.class,
                withSettings().useConstructor(roundingMultiple).defaultAnswer(RETURNS_DEFAULTS));
        when(roundTargetQueueLength.getRoundedTargetQueueLength(300)).thenReturn(300L);
        when(roundTargetQueueLength.getRoundedTargetQueueLength(3000)).thenReturn(3000L);

        final TargetQueuePerformanceMetricsProvider targetQueuePerformanceMetricsProvider =
                mock(TargetQueuePerformanceMetricsProvider.class);
        when(targetQueuePerformanceMetricsProvider.getTargetQueuePerformanceMetrics(targetQueue1))
                .thenReturn(consumptionRate1);
        when(targetQueuePerformanceMetricsProvider.getTargetQueuePerformanceMetrics(targetQueue2))
                .thenReturn(consumptionRate2);

        final TunedTargetQueueLengthProvider targetQueue = new TunedTargetQueueLengthProvider(targetQueuePerformanceMetricsProvider,
                historicalConsumptionRate, roundTargetQueueLength, false, queueProcessingTimeGoalSeconds);

        final long tunedTargetQueueLength1 = getTunedTargetQueueLength(targetQueue1, targetQueue, targetQueueSettings);
        targetQueueSettings.setCurrentMaxLength(tunedTargetQueueLength1);
        final long repeatTunedTargetQueueLength = getTunedTargetQueueLength(targetQueue1, targetQueue, targetQueueSettings);

        final long tunedTargetQueueLength2 = getTunedTargetQueueLength(targetQueue2, targetQueue, targetQueueSettings);

        assertEquals("isSufficientHistoryAvailable set to true: Consumption rate history has been provided: Target queue length " +
                "should be adjusted. Once the tuned target queue length has been adjusted, when trying to get the target tuned length " +
                "again, the logs should state 'Target queue is already set to optimum length: 300. No action required.' ",
                300, repeatTunedTargetQueueLength);
        assertEquals("isSufficientHistoryAvailable set to true: Consumption rate history has been provided: Target queue length " +
                "should be adjusted.", 3000, tunedTargetQueueLength2);
    }

    @Test
    public void getMinAndMaxTunedTargetQueueForQueueWithAdequateHistoryAndNoOpModeOffTest(){

        final double consumptionRate1 = 0.00005;
        final double consumptionRate2 = 5000;

        final TargetQueueSettings targetQueueSettings = new TargetQueueSettings(targetQueueLength, eligableForRefillPercentage,
                maxInstances, currentInstances);

        final HistoricalConsumptionRate historicalConsumptionRate = mock(HistoricalConsumptionRate.class);
        when(historicalConsumptionRate.isSufficientHistoryAvailable(targetQueue1)).thenReturn(true);
        when(historicalConsumptionRate.isSufficientHistoryAvailable(targetQueue2)).thenReturn(true);
        when(historicalConsumptionRate.recordCurrentConsumptionRateHistoryAndGetAverage(targetQueue1,
                0.01)).thenReturn(0.01);
        when(historicalConsumptionRate.recordCurrentConsumptionRateHistoryAndGetAverage(targetQueue2,
                10000.0)).thenReturn(10000.0);

        final RoundTargetQueueLength roundTargetQueueLength = mock(RoundTargetQueueLength.class,
                withSettings().useConstructor(roundingMultiple).defaultAnswer(RETURNS_DEFAULTS));
        when(roundTargetQueueLength.getRoundedTargetQueueLength(3)).thenReturn(0L);
        when(roundTargetQueueLength.getRoundedTargetQueueLength(3000000)).thenReturn(3000000L);

        final TargetQueuePerformanceMetricsProvider targetQueuePerformanceMetricsProvider =
                mock(TargetQueuePerformanceMetricsProvider.class);
        when(targetQueuePerformanceMetricsProvider.getTargetQueuePerformanceMetrics(targetQueue1))
                .thenReturn(consumptionRate1);
        when(targetQueuePerformanceMetricsProvider.getTargetQueuePerformanceMetrics(targetQueue2))
                .thenReturn(consumptionRate2);

        final TunedTargetQueueLengthProvider targetQueue = new TunedTargetQueueLengthProvider(targetQueuePerformanceMetricsProvider,
                historicalConsumptionRate, roundTargetQueueLength, false, queueProcessingTimeGoalSeconds);

        final long tunedTargetQueueLength1 = getTunedTargetQueueLength(targetQueue1, targetQueue, targetQueueSettings);
        final long tunedTargetQueueLength2 = getTunedTargetQueueLength(targetQueue2, targetQueue, targetQueueSettings);

        assertEquals("isSufficientHistoryAvailable set to true: Consumption rate history has been provided: Target queue length " +
                "should be adjusted to the minimum.", minTargetQueueLength, tunedTargetQueueLength1);
        assertEquals("isSufficientHistoryAvailable set to true: Consumption rate history has been provided: Target queue length " +
                "should be adjusted to the maximum.", maxTargetQueueLength, tunedTargetQueueLength2);
    }

    private long getTunedTargetQueueLength(final String queueName, final TunedTargetQueueLengthProvider targetQueue,
                                           final TargetQueueSettings targetQueueSettings) {
        return targetQueue.getTunedTargetQueueLength(queueName, minTargetQueueLength, maxTargetQueueLength, targetQueueSettings);
    }

}
