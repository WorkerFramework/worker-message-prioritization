/*
 * Copyright 2022-2024 Open Text.
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
import com.github.workerframework.workermessageprioritization.targetqueue.HistoricalConsumptionRateManager;
import com.github.workerframework.workermessageprioritization.targetqueue.QueueInformationProvider;
import com.github.workerframework.workermessageprioritization.targetqueue.TargetQueueLengthRounder;
import com.github.workerframework.workermessageprioritization.targetqueue.TargetQueueSettings;
import static org.junit.jupiter.api.Assertions.assertEquals;
import org.junit.jupiter.api.Test;

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
    public void getTunedTargetQueueTuningDisabledTest(){

        final double consumptionRate = 0.5;

        final TargetQueueSettings targetQueueSettings = new TargetQueueSettings(targetQueueLength, eligableForRefillPercentage,
                maxInstances, currentInstances, targetQueueLength);

        final HistoricalConsumptionRateManager historicalConsumptionRateManager = mock(HistoricalConsumptionRateManager.class);
        when(historicalConsumptionRateManager.isSufficientHistoryAvailable(ArgumentMatchers.anyString())).thenReturn(false);
        when(historicalConsumptionRateManager.recordCurrentConsumptionRateHistoryAndGetAverage(ArgumentMatchers.anyString(),
                ArgumentMatchers.anyDouble(), ArgumentMatchers.anyDouble())).thenReturn(1.0);

        final TargetQueueLengthRounder targetQueueLengthRounder = mock(TargetQueueLengthRounder.class,
                withSettings().useConstructor(roundingMultiple).defaultAnswer(RETURNS_DEFAULTS));
        when(targetQueueLengthRounder.getRoundedTargetQueueLength(ArgumentMatchers.anyLong())).thenReturn(300L);

        final QueueInformationProvider queueInformationProvider =
                mock(QueueInformationProvider.class);
        when(queueInformationProvider.getConsumptionRate(targetQueue1))
                .thenReturn(consumptionRate);
        when(queueInformationProvider.getMessageBytesReady(targetQueue1))
                .thenReturn(1D);

        final TunedTargetQueueLengthProvider targetQueue = new TunedTargetQueueLengthProvider(queueInformationProvider,
                historicalConsumptionRateManager, targetQueueLengthRounder, minTargetQueueLength, maxTargetQueueLength,
                false, queueProcessingTimeGoalSeconds);

        final long tunedTargetQueueLength = getTunedTargetQueueLength(targetQueue1, targetQueue, targetQueueSettings);

        assertEquals(targetQueueLength, tunedTargetQueueLength, 
                "Tuning is disabled: Target queue length should not have changed. Suggested adjustment for the target queue length" +
                " should still be logged.");
    }

    @Test
    public void getTunedTargetQueueForQueueWithInadequateHistoryAndTuningEnabledTest(){

        final double consumptionRate = 0.5;

        final TargetQueueSettings targetQueueSettings = new TargetQueueSettings(targetQueueLength, eligableForRefillPercentage,
                maxInstances, currentInstances, targetQueueLength);

        final HistoricalConsumptionRateManager historicalConsumptionRateManager = mock(HistoricalConsumptionRateManager.class);
        when(historicalConsumptionRateManager.isSufficientHistoryAvailable(ArgumentMatchers.anyString())).thenReturn(false);
        when(historicalConsumptionRateManager.recordCurrentConsumptionRateHistoryAndGetAverage(ArgumentMatchers.anyString(),
                ArgumentMatchers.anyDouble(), ArgumentMatchers.anyDouble())).thenReturn(1.0);

        final TargetQueueLengthRounder targetQueueLengthRounder = mock(TargetQueueLengthRounder.class,
                withSettings().useConstructor(roundingMultiple).defaultAnswer(RETURNS_DEFAULTS));
        when(targetQueueLengthRounder.getRoundedTargetQueueLength(ArgumentMatchers.anyLong())).thenReturn(300L);

        final QueueInformationProvider queueInformationProvider =
                mock(QueueInformationProvider.class);
        when(queueInformationProvider.getConsumptionRate(targetQueue1))
                .thenReturn(consumptionRate);
        when(queueInformationProvider.getMessageBytesReady(targetQueue1))
                .thenReturn(1D);

        final TunedTargetQueueLengthProvider targetQueue = new TunedTargetQueueLengthProvider(queueInformationProvider,
                historicalConsumptionRateManager, targetQueueLengthRounder, minTargetQueueLength, maxTargetQueueLength,
                true, queueProcessingTimeGoalSeconds);

        final long tunedTargetQueueLength = getTunedTargetQueueLength(targetQueue1, targetQueue, targetQueueSettings);

        assertEquals(targetQueueLength, tunedTargetQueueLength, 
                "isSufficientHistoryAvailable set to false: Not enough consumption rate history. Target queue length should not " +
                        "change.");
    }


    @Test
    public void getDifferentTunedTargetQueueLengthsForQueuesWithAdequateHistoryAndTuningEnabledTest(){

        final double consumptionRate1 = 0.5;
        final double consumptionRate2 = 5;

        final double messageBytesReady = 1D;

        final TargetQueueSettings targetQueueSettings = new TargetQueueSettings(targetQueueLength, eligableForRefillPercentage,
                maxInstances, currentInstances, targetQueueLength);

        final HistoricalConsumptionRateManager historicalConsumptionRateManager = mock(HistoricalConsumptionRateManager.class);
        when(historicalConsumptionRateManager.isSufficientHistoryAvailable(targetQueue1)).thenReturn(true);
        when(historicalConsumptionRateManager.isSufficientHistoryAvailable(targetQueue2)).thenReturn(true);
        when(historicalConsumptionRateManager.recordCurrentConsumptionRateHistoryAndGetAverage(targetQueue1,
                1.0, messageBytesReady)).thenReturn(1.0);
        when(historicalConsumptionRateManager.recordCurrentConsumptionRateHistoryAndGetAverage(targetQueue2,
                10.0, messageBytesReady)).thenReturn(10.0);

        final TargetQueueLengthRounder targetQueueLengthRounder = mock(TargetQueueLengthRounder.class,
                withSettings().useConstructor(roundingMultiple).defaultAnswer(RETURNS_DEFAULTS));
        when(targetQueueLengthRounder.getRoundedTargetQueueLength(300)).thenReturn(300L);
        when(targetQueueLengthRounder.getRoundedTargetQueueLength(3000)).thenReturn(3000L);

        final QueueInformationProvider queueInformationProvider =
                mock(QueueInformationProvider.class);
        when(queueInformationProvider.getConsumptionRate(targetQueue1))
                .thenReturn(consumptionRate1);
        when(queueInformationProvider.getMessageBytesReady(targetQueue1))
                .thenReturn(1D);
        when(queueInformationProvider.getConsumptionRate(targetQueue2))
                .thenReturn(consumptionRate2);
        when(queueInformationProvider.getMessageBytesReady(targetQueue2))
                .thenReturn(1D);

        final TunedTargetQueueLengthProvider targetQueue = new TunedTargetQueueLengthProvider(queueInformationProvider,
                historicalConsumptionRateManager, targetQueueLengthRounder, minTargetQueueLength, maxTargetQueueLength,
                true, queueProcessingTimeGoalSeconds);

        final long tunedTargetQueueLength1 = getTunedTargetQueueLength(targetQueue1, targetQueue, targetQueueSettings);
        targetQueueSettings.setCurrentMaxLength(tunedTargetQueueLength1);
        final long repeatTunedTargetQueueLength = getTunedTargetQueueLength(targetQueue1, targetQueue, targetQueueSettings);

        final long tunedTargetQueueLength2 = getTunedTargetQueueLength(targetQueue2, targetQueue, targetQueueSettings);

        assertEquals(300, repeatTunedTargetQueueLength,
                "isSufficientHistoryAvailable set to true: Consumption rate history has been provided: Target queue length " +
                "should be adjusted. Once the tuned target queue length has been adjusted, when trying to get the target tuned length " +
                "again, the logs should state 'Target queue is already set to optimum length: 300. No action required.' ");
        assertEquals(3000, tunedTargetQueueLength2,
                "isSufficientHistoryAvailable set to true: Consumption rate history has been provided: Target queue length " +
                "should be adjusted.");
    }

    @Test
    public void getMinAndMaxTunedTargetQueueForQueueWithAdequateHistoryAndTuningEnabledTest(){

        final double consumptionRate1 = 0.00005;
        final double consumptionRate2 = 5000;

        final double messageBytesReady = 1D;

        final TargetQueueSettings targetQueueSettings = new TargetQueueSettings(targetQueueLength, eligableForRefillPercentage,
                maxInstances, currentInstances, targetQueueLength);

        final HistoricalConsumptionRateManager historicalConsumptionRateManager = mock(HistoricalConsumptionRateManager.class);
        when(historicalConsumptionRateManager.isSufficientHistoryAvailable(targetQueue1)).thenReturn(true);
        when(historicalConsumptionRateManager.isSufficientHistoryAvailable(targetQueue2)).thenReturn(true);
        when(historicalConsumptionRateManager.recordCurrentConsumptionRateHistoryAndGetAverage(targetQueue1,
                0.01, messageBytesReady)).thenReturn(0.01);
        when(historicalConsumptionRateManager.recordCurrentConsumptionRateHistoryAndGetAverage(targetQueue2,
                10000.0, messageBytesReady)).thenReturn(10000.0);

        final TargetQueueLengthRounder targetQueueLengthRounder = mock(TargetQueueLengthRounder.class,
                withSettings().useConstructor(roundingMultiple).defaultAnswer(RETURNS_DEFAULTS));
        when(targetQueueLengthRounder.getRoundedTargetQueueLength(3)).thenReturn(0L);
        when(targetQueueLengthRounder.getRoundedTargetQueueLength(3000000)).thenReturn(3000000L);

        final QueueInformationProvider queueInformationProvider =
                mock(QueueInformationProvider.class);
        when(queueInformationProvider.getConsumptionRate(targetQueue1))
                .thenReturn(consumptionRate1);
        when(queueInformationProvider.getMessageBytesReady(targetQueue1))
                .thenReturn(1D);
        when(queueInformationProvider.getConsumptionRate(targetQueue2))
                .thenReturn(consumptionRate2);
        when(queueInformationProvider.getMessageBytesReady(targetQueue2))
                .thenReturn(1D);

        final TunedTargetQueueLengthProvider targetQueue = new TunedTargetQueueLengthProvider(queueInformationProvider,
                historicalConsumptionRateManager, targetQueueLengthRounder, minTargetQueueLength, maxTargetQueueLength,
                true, queueProcessingTimeGoalSeconds);

        final long tunedTargetQueueLength1 = getTunedTargetQueueLength(targetQueue1, targetQueue, targetQueueSettings);
        final long tunedTargetQueueLength2 = getTunedTargetQueueLength(targetQueue2, targetQueue, targetQueueSettings);

        assertEquals(minTargetQueueLength, tunedTargetQueueLength1,
                "isSufficientHistoryAvailable set to true: Consumption rate history has been provided: Target queue length " +
                "should be adjusted to the minimum.");
        assertEquals(maxTargetQueueLength, tunedTargetQueueLength2,
                "isSufficientHistoryAvailable set to true: Consumption rate history has been provided: Target queue length " +
                "should be adjusted to the maximum.");
    }

    private long getTunedTargetQueueLength(final String queueName, final TunedTargetQueueLengthProvider targetQueue,
                                           final TargetQueueSettings targetQueueSettings) {
        return targetQueue.getTunedTargetQueueLength(queueName, targetQueueSettings);
    }

}
