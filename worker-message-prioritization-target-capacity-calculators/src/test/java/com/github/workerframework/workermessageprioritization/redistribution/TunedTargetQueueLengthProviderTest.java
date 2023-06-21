package com.github.workerframework.workermessageprioritization.redistribution;

import com.github.workerframework.workermessageprioritization.targetqueue.TunedTargetQueueLengthProvider;
import com.github.workerframework.workermessageprioritization.targetqueue.HistoricalConsumptionRate;
import com.github.workerframework.workermessageprioritization.targetqueue.TargetQueuePerformanceMetricsProvider;
import com.github.workerframework.workermessageprioritization.targetqueue.RoundTargetQueueLength;
import com.github.workerframework.workermessageprioritization.targetqueue.PerformanceMetrics;
import org.junit.Test;
import org.mockito.ArgumentMatchers;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TunedTargetQueueLengthProviderTest {

    private final long targetQueueLength = 10000;
    private final String targetQueue1 = "targetQueue1";
    private final String targetQueue2 = "targetQueue2";
    private final long minTargetQueueLength = 100;
    private final long maxTargetQueueLength = 1000000;
    private final long queueProcessingTimeGoalSeconds = 300; //5 minutes
    private final double currentInstances = 2;
    private final double maxInstances = 4;

    @Test
    public void getTunedTargetQueueNoOpTest(){

        final double consumptionRate = 0.5;
        final long tunedTargetQueueLength;

        final PerformanceMetrics performanceMetrics = new PerformanceMetrics(targetQueueLength, consumptionRate, currentInstances,
                maxInstances);

        final HistoricalConsumptionRate historicalConsumptionRate = mock(HistoricalConsumptionRate.class);
        when(historicalConsumptionRate.isSufficientHistoryAvailable(ArgumentMatchers.anyString())).thenReturn(false);
        when(historicalConsumptionRate.recordCurrentConsumptionRateHistoryAndGetAverage(ArgumentMatchers.anyString(),
                ArgumentMatchers.anyDouble())).thenReturn(1.0);

        final RoundTargetQueueLength roundTargetQueueLength = mock(RoundTargetQueueLength.class);
        when(roundTargetQueueLength.getRoundedTargetQueueLength(ArgumentMatchers.anyLong())).thenReturn(300L);

        final TargetQueuePerformanceMetricsProvider targetQueuePerformanceMetricsProvider =
                mock(TargetQueuePerformanceMetricsProvider.class);
        when(targetQueuePerformanceMetricsProvider.getTargetQueuePerformanceMetrics(targetQueue1)).thenReturn(performanceMetrics);

        final TunedTargetQueueLengthProvider targetQueue = new TunedTargetQueueLengthProvider(targetQueuePerformanceMetricsProvider,
                historicalConsumptionRate, roundTargetQueueLength, true, queueProcessingTimeGoalSeconds);

        tunedTargetQueueLength = getTunedTargetQueueLength(targetQueue1,targetQueue);

        assertEquals("NoOpMode is on: Target queue length should not have changed.Suggested adjustment for the target queue length " +
                "should still be logged.", targetQueueLength, tunedTargetQueueLength);
    }

    @Test
    public void getMaxTunedTargetQueueNoOpTest(){

        final double consumptionRate = 50;
        final long tunedTargetQueueLength;

        final PerformanceMetrics performanceMetrics = new PerformanceMetrics(targetQueueLength, consumptionRate, currentInstances,
                maxInstances);

        final HistoricalConsumptionRate historicalConsumptionRate = mock(HistoricalConsumptionRate.class);
        when(historicalConsumptionRate.isSufficientHistoryAvailable(ArgumentMatchers.anyString())).thenReturn(false);
        when(historicalConsumptionRate.recordCurrentConsumptionRateHistoryAndGetAverage(ArgumentMatchers.anyString(),
                ArgumentMatchers.anyDouble())).thenReturn(100.0);

        final RoundTargetQueueLength roundTargetQueueLength = mock(RoundTargetQueueLength.class);
        when(roundTargetQueueLength.getRoundedTargetQueueLength(ArgumentMatchers.anyLong())).thenReturn(30000L);

        final TargetQueuePerformanceMetricsProvider targetQueuePerformanceMetricsProvider =
                mock(TargetQueuePerformanceMetricsProvider.class);
        when(targetQueuePerformanceMetricsProvider.getTargetQueuePerformanceMetrics(targetQueue1)).thenReturn(performanceMetrics);

        final TunedTargetQueueLengthProvider targetQueue = new TunedTargetQueueLengthProvider(targetQueuePerformanceMetricsProvider,
                historicalConsumptionRate, roundTargetQueueLength, true, queueProcessingTimeGoalSeconds);

        tunedTargetQueueLength = getTunedTargetQueueLength(targetQueue1,targetQueue);

        assertEquals("NoOpMode is on: Target queue length should not have changed. Suggested adjustment for the target queue" +
                " length should still be logged.", targetQueueLength, tunedTargetQueueLength);
    }

    @Test
    public void getMinTunedTargetQueueNoOpTest(){

        final double consumptionRate = 0.05;
        final long tunedTargetQueueLength;

        final PerformanceMetrics performanceMetrics = new PerformanceMetrics(targetQueueLength, consumptionRate, currentInstances,
                maxInstances);

        final HistoricalConsumptionRate historicalConsumptionRate = mock(HistoricalConsumptionRate.class);
        when(historicalConsumptionRate.isSufficientHistoryAvailable(ArgumentMatchers.anyString())).thenReturn(false);
        when(historicalConsumptionRate.recordCurrentConsumptionRateHistoryAndGetAverage(ArgumentMatchers.anyString(),
                ArgumentMatchers.anyDouble())).thenReturn(0.01);

        final RoundTargetQueueLength roundTargetQueueLength = mock(RoundTargetQueueLength.class);
        when(roundTargetQueueLength.getRoundedTargetQueueLength(ArgumentMatchers.anyLong())).thenReturn(0L);

        final TargetQueuePerformanceMetricsProvider targetQueuePerformanceMetricsProvider =
                mock(TargetQueuePerformanceMetricsProvider.class);
        when(targetQueuePerformanceMetricsProvider.getTargetQueuePerformanceMetrics(targetQueue1)).thenReturn(performanceMetrics);

        final TunedTargetQueueLengthProvider targetQueue = new TunedTargetQueueLengthProvider(targetQueuePerformanceMetricsProvider,
                historicalConsumptionRate, roundTargetQueueLength, true, queueProcessingTimeGoalSeconds);

        tunedTargetQueueLength = getTunedTargetQueueLength(targetQueue1,targetQueue);

        assertEquals("NoOpMode is on: Target queue length should not have changed. Suggested adjustment for the target queue " +
                "length should still be logged.", targetQueueLength, tunedTargetQueueLength);
    }

    @Test
    public void getTunedTargetQueueForQueueWithInadequateHistoryAndNoOpModeOffTest(){

        final double consumptionRate = 0.5;
        final long tunedTargetQueueLength;

        final PerformanceMetrics performanceMetrics = new PerformanceMetrics(targetQueueLength, consumptionRate, currentInstances,
                maxInstances);

        final HistoricalConsumptionRate historicalConsumptionRate = mock(HistoricalConsumptionRate.class);
        when(historicalConsumptionRate.isSufficientHistoryAvailable(ArgumentMatchers.anyString())).thenReturn(false);
        when(historicalConsumptionRate.recordCurrentConsumptionRateHistoryAndGetAverage(ArgumentMatchers.anyString(),
                ArgumentMatchers.anyDouble())).thenReturn(1.0);

        final RoundTargetQueueLength roundTargetQueueLength = mock(RoundTargetQueueLength.class);
        when(roundTargetQueueLength.getRoundedTargetQueueLength(ArgumentMatchers.anyLong())).thenReturn(300L);

        final TargetQueuePerformanceMetricsProvider targetQueuePerformanceMetricsProvider =
                mock(TargetQueuePerformanceMetricsProvider.class);
        when(targetQueuePerformanceMetricsProvider.getTargetQueuePerformanceMetrics(targetQueue1)).thenReturn(performanceMetrics);

        final TunedTargetQueueLengthProvider targetQueue = new TunedTargetQueueLengthProvider(targetQueuePerformanceMetricsProvider,
                historicalConsumptionRate, roundTargetQueueLength, false, queueProcessingTimeGoalSeconds);

        tunedTargetQueueLength = getTunedTargetQueueLength(targetQueue1,targetQueue);

        assertEquals("isSufficientHistoryAvailable set to false: Not enough consumption rate history. Target queue length should not " +
                        "change.", targetQueueLength,
                tunedTargetQueueLength);
    }

    @Test
    public void getMaxTunedTargetQueueForQueueWithInadequateHistoryAndNoOpModeOffTest(){

        final double consumptionRate = 5000;
        final long tunedTargetQueueLength;

        final PerformanceMetrics performanceMetrics = new PerformanceMetrics(targetQueueLength, consumptionRate, currentInstances,
                maxInstances);

        final HistoricalConsumptionRate historicalConsumptionRate = mock(HistoricalConsumptionRate.class);
        when(historicalConsumptionRate.isSufficientHistoryAvailable(ArgumentMatchers.anyString())).thenReturn(false);
        when(historicalConsumptionRate.recordCurrentConsumptionRateHistoryAndGetAverage(ArgumentMatchers.anyString(),
                ArgumentMatchers.anyDouble())).thenReturn(100.0);

        final RoundTargetQueueLength roundTargetQueueLength = mock(RoundTargetQueueLength.class);
        when(roundTargetQueueLength.getRoundedTargetQueueLength(ArgumentMatchers.anyLong())).thenReturn(30000L);

        final TargetQueuePerformanceMetricsProvider targetQueuePerformanceMetricsProvider =
                mock(TargetQueuePerformanceMetricsProvider.class);
        when(targetQueuePerformanceMetricsProvider.getTargetQueuePerformanceMetrics(targetQueue1)).thenReturn(performanceMetrics);

        final TunedTargetQueueLengthProvider targetQueue = new TunedTargetQueueLengthProvider(targetQueuePerformanceMetricsProvider,
                historicalConsumptionRate, roundTargetQueueLength, false, queueProcessingTimeGoalSeconds);

        tunedTargetQueueLength = getTunedTargetQueueLength(targetQueue1,targetQueue);

        assertEquals("isSufficientHistoryAvailable set to false: Not enough consumption rate history. Target queue length should not " +
                        "change.", targetQueueLength, tunedTargetQueueLength);
    }

    @Test
    public void getMinTunedTargetQueueForQueueWithInadequateHistoryAndNoOpModeOffTest(){

        final double consumptionRate = 0.00005;
        final long tunedTargetQueueLength;

        final PerformanceMetrics performanceMetrics = new PerformanceMetrics(targetQueueLength, consumptionRate, currentInstances,
                maxInstances);

        final HistoricalConsumptionRate historicalConsumptionRate = mock(HistoricalConsumptionRate.class);
        when(historicalConsumptionRate.isSufficientHistoryAvailable(ArgumentMatchers.anyString())).thenReturn(false);
        when(historicalConsumptionRate.recordCurrentConsumptionRateHistoryAndGetAverage(ArgumentMatchers.anyString(),
                ArgumentMatchers.anyDouble())).thenReturn(0.01);

        final RoundTargetQueueLength roundTargetQueueLength = mock(RoundTargetQueueLength.class);
        when(roundTargetQueueLength.getRoundedTargetQueueLength(ArgumentMatchers.anyLong())).thenReturn(0L);

        final TargetQueuePerformanceMetricsProvider targetQueuePerformanceMetricsProvider =
                mock(TargetQueuePerformanceMetricsProvider.class);
        when(targetQueuePerformanceMetricsProvider.getTargetQueuePerformanceMetrics(targetQueue1)).thenReturn(performanceMetrics);

        final TunedTargetQueueLengthProvider targetQueue = new TunedTargetQueueLengthProvider(targetQueuePerformanceMetricsProvider,
                historicalConsumptionRate, roundTargetQueueLength, false, queueProcessingTimeGoalSeconds);

        tunedTargetQueueLength = getTunedTargetQueueLength(targetQueue1,targetQueue);

        assertEquals("isSufficientHistoryAvailable set to false: Not enough consumption rate history. Target queue length should not " +
                        "change.", targetQueueLength, tunedTargetQueueLength);
    }

    @Test
    public void getTunedTargetQueueForQueueWithAdequateHistoryAndNoOpModeOffTest(){

        final double consumptionRate = 0.5;
        final long tunedTargetQueueLength;

        final PerformanceMetrics performanceMetrics = new PerformanceMetrics(targetQueueLength, consumptionRate, currentInstances,
                maxInstances);

        final HistoricalConsumptionRate historicalConsumptionRate = mock(HistoricalConsumptionRate.class);
        when(historicalConsumptionRate.isSufficientHistoryAvailable(ArgumentMatchers.anyString())).thenReturn(true);
        when(historicalConsumptionRate.recordCurrentConsumptionRateHistoryAndGetAverage(ArgumentMatchers.anyString(),
                ArgumentMatchers.anyDouble())).thenReturn(1.0);

        final RoundTargetQueueLength roundTargetQueueLength = mock(RoundTargetQueueLength.class);
        when(roundTargetQueueLength.getRoundedTargetQueueLength(ArgumentMatchers.anyLong())).thenReturn(300L);

        final TargetQueuePerformanceMetricsProvider targetQueuePerformanceMetricsProvider =
                mock(TargetQueuePerformanceMetricsProvider.class);
        when(targetQueuePerformanceMetricsProvider.getTargetQueuePerformanceMetrics(targetQueue1)).thenReturn(performanceMetrics);

        final TunedTargetQueueLengthProvider targetQueue = new TunedTargetQueueLengthProvider(targetQueuePerformanceMetricsProvider,
                historicalConsumptionRate, roundTargetQueueLength, false, queueProcessingTimeGoalSeconds);

        tunedTargetQueueLength = getTunedTargetQueueLength(targetQueue1,targetQueue);

        assertEquals("isSufficientHistoryAvailable set to true: Consumption rate history has been provided. Target queue length " +
                        "should be adjusted.", 300, tunedTargetQueueLength);
    }

    @Test
    public void getMaxTunedTargetQueueForQueueWithAdequateHistoryAndNoOpModeOffTest(){

        final double consumptionRate = 5000;
        final long tunedTargetQueueLength;

        final PerformanceMetrics performanceMetrics = new PerformanceMetrics(targetQueueLength, consumptionRate, currentInstances,
                maxInstances);

        final HistoricalConsumptionRate historicalConsumptionRate = mock(HistoricalConsumptionRate.class);
        when(historicalConsumptionRate.isSufficientHistoryAvailable(ArgumentMatchers.anyString())).thenReturn(true);
        when(historicalConsumptionRate.recordCurrentConsumptionRateHistoryAndGetAverage(ArgumentMatchers.anyString(),
                ArgumentMatchers.anyDouble())).thenReturn(100.0);

        final RoundTargetQueueLength roundTargetQueueLength = mock(RoundTargetQueueLength.class);
        when(roundTargetQueueLength.getRoundedTargetQueueLength(ArgumentMatchers.anyLong())).thenReturn(3000000L);

        final TargetQueuePerformanceMetricsProvider targetQueuePerformanceMetricsProvider =
                mock(TargetQueuePerformanceMetricsProvider.class);
        when(targetQueuePerformanceMetricsProvider.getTargetQueuePerformanceMetrics(targetQueue1)).thenReturn(performanceMetrics);

        final TunedTargetQueueLengthProvider targetQueue = new TunedTargetQueueLengthProvider(targetQueuePerformanceMetricsProvider,
                historicalConsumptionRate, roundTargetQueueLength, false, queueProcessingTimeGoalSeconds);

        tunedTargetQueueLength = getTunedTargetQueueLength(targetQueue1,targetQueue);

        assertEquals("isSufficientHistoryAvailable set to true: Consumption rate history has been provided: Target queue length " +
                        "should be adjusted to the maximum.", maxTargetQueueLength, tunedTargetQueueLength);
    }

    @Test
    public void getMinTunedTargetQueueForQueueWithAdequateHistoryAndNoOpModeOffTest(){

        final double consumptionRate = 0.00005;
        final long tunedTargetQueueLength;

        final PerformanceMetrics performanceMetrics = new PerformanceMetrics(targetQueueLength, consumptionRate, currentInstances,
                maxInstances);

        final HistoricalConsumptionRate historicalConsumptionRate = mock(HistoricalConsumptionRate.class);
        when(historicalConsumptionRate.isSufficientHistoryAvailable(ArgumentMatchers.anyString())).thenReturn(true);
        when(historicalConsumptionRate.recordCurrentConsumptionRateHistoryAndGetAverage(ArgumentMatchers.anyString(),
                ArgumentMatchers.anyDouble())).thenReturn(0.01);

        final RoundTargetQueueLength roundTargetQueueLength = mock(RoundTargetQueueLength.class);
        when(roundTargetQueueLength.getRoundedTargetQueueLength(ArgumentMatchers.anyLong())).thenReturn(0L);

        final TargetQueuePerformanceMetricsProvider targetQueuePerformanceMetricsProvider =
                mock(TargetQueuePerformanceMetricsProvider.class);
        when(targetQueuePerformanceMetricsProvider.getTargetQueuePerformanceMetrics(targetQueue1)).thenReturn(performanceMetrics);

        final TunedTargetQueueLengthProvider targetQueue = new TunedTargetQueueLengthProvider(targetQueuePerformanceMetricsProvider,
                historicalConsumptionRate, roundTargetQueueLength, false, queueProcessingTimeGoalSeconds);

        tunedTargetQueueLength = getTunedTargetQueueLength(targetQueue1,targetQueue);

        assertEquals("isSufficientHistoryAvailable set to true: Consumption rate history has been provided: Target queue length " +
                        "should be adjusted to the minimum.", minTargetQueueLength, tunedTargetQueueLength);
    }

    @Test
    public void getDifferentTunedTargetQueueLengthsForQueuesWithAdequateHistoryAndNoOpModeOffTest(){

        final double consumptionRate1 = 0.5;
        final double consumptionRate2 = 5;

        long tunedTargetQueueLength1;
        long tunedTargetQueueLength2;

        final PerformanceMetrics performanceMetrics1 = new PerformanceMetrics(targetQueueLength, consumptionRate1, currentInstances,
                maxInstances);
        final PerformanceMetrics performanceMetrics2 = new PerformanceMetrics(targetQueueLength, consumptionRate2, currentInstances,
                maxInstances);

        final HistoricalConsumptionRate historicalConsumptionRate = mock(HistoricalConsumptionRate.class);
        when(historicalConsumptionRate.isSufficientHistoryAvailable(targetQueue1)).thenReturn(true);
        when(historicalConsumptionRate.isSufficientHistoryAvailable(targetQueue2)).thenReturn(true);
        when(historicalConsumptionRate.recordCurrentConsumptionRateHistoryAndGetAverage(targetQueue1,
                1.0)).thenReturn(1.0);
        when(historicalConsumptionRate.recordCurrentConsumptionRateHistoryAndGetAverage(targetQueue2,
                10.0)).thenReturn(10.0);

        final RoundTargetQueueLength roundTargetQueueLength = mock(RoundTargetQueueLength.class);
        when(roundTargetQueueLength.getRoundedTargetQueueLength(300)).thenReturn(300L);
        when(roundTargetQueueLength.getRoundedTargetQueueLength(3000)).thenReturn(3000L);

        final TargetQueuePerformanceMetricsProvider targetQueuePerformanceMetricsProvider =
                mock(TargetQueuePerformanceMetricsProvider.class);
        when(targetQueuePerformanceMetricsProvider.getTargetQueuePerformanceMetrics(targetQueue1)).thenReturn(performanceMetrics1);
        when(targetQueuePerformanceMetricsProvider.getTargetQueuePerformanceMetrics(targetQueue2)).thenReturn(performanceMetrics2);

        final TunedTargetQueueLengthProvider targetQueue = new TunedTargetQueueLengthProvider(targetQueuePerformanceMetricsProvider,
                historicalConsumptionRate, roundTargetQueueLength, false, queueProcessingTimeGoalSeconds);

        tunedTargetQueueLength1 = getTunedTargetQueueLength(targetQueue1,targetQueue);
        tunedTargetQueueLength2 = getTunedTargetQueueLength(targetQueue2,targetQueue);

        assertEquals("isSufficientHistoryAvailable set to true: Consumption rate history has been provided: Target queue length " +
                        "should be adjusted.", 300, tunedTargetQueueLength1);
        assertEquals("isSufficientHistoryAvailable set to true: Consumption rate history has been provided: Target queue length " +
                        "should be adjusted.", 3000, tunedTargetQueueLength2);
    }

    @Test
    public void getRepeatTunedTargetQueueForQueueWithAdequateHistoryAndNoOpModeOffTest(){

        final double consumptionRate = 0.5;
        final long tunedTargetQueueLength;

        final PerformanceMetrics performanceMetrics = new PerformanceMetrics(targetQueueLength, consumptionRate, currentInstances,
                maxInstances);

        final HistoricalConsumptionRate historicalConsumptionRate = mock(HistoricalConsumptionRate.class);
        when(historicalConsumptionRate.isSufficientHistoryAvailable(ArgumentMatchers.anyString())).thenReturn(true);
        when(historicalConsumptionRate.recordCurrentConsumptionRateHistoryAndGetAverage(ArgumentMatchers.anyString(),
                ArgumentMatchers.anyDouble())).thenReturn(1.0);

        final RoundTargetQueueLength roundTargetQueueLength = mock(RoundTargetQueueLength.class);
        when(roundTargetQueueLength.getRoundedTargetQueueLength(ArgumentMatchers.anyLong())).thenReturn(300L);

        final TargetQueuePerformanceMetricsProvider targetQueuePerformanceMetricsProvider =
                mock(TargetQueuePerformanceMetricsProvider.class);
        when(targetQueuePerformanceMetricsProvider.getTargetQueuePerformanceMetrics(targetQueue1)).thenReturn(performanceMetrics);

        final TunedTargetQueueLengthProvider targetQueue = new TunedTargetQueueLengthProvider(targetQueuePerformanceMetricsProvider,
                historicalConsumptionRate, roundTargetQueueLength, false, queueProcessingTimeGoalSeconds);

        tunedTargetQueueLength = getTunedTargetQueueLength(targetQueue1,targetQueue);
        performanceMetrics.setTargetQueueLength(tunedTargetQueueLength);
        long repeatTunedTargetQueueLength = getTunedTargetQueueLength(targetQueue1,targetQueue);

        assertEquals("isSufficientHistoryAvailable set to true: Consumption rate history has been provided. Target queue length " +
                "should be adjusted. Once the tuned target queue length has been adjusted, when trying to get the target tuned length " +
                        "again, the logs should state 'Target queue is already set to optimum length: 300. No action required.'", 300,
                repeatTunedTargetQueueLength);
    }

    private long getTunedTargetQueueLength(final String queueName, final TunedTargetQueueLengthProvider targetQueue) {
        return targetQueue.getTunedTargetQueueLength(queueName, minTargetQueueLength, maxTargetQueueLength);
    }

}
