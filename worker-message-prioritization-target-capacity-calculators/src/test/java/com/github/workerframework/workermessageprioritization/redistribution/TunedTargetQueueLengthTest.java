package com.github.workerframework.workermessageprioritization.redistribution;

import com.github.workerframework.workermessageprioritization.rabbitmq.Queue;
import com.github.workerframework.workermessageprioritization.targetqueue.TargetQueuePerformanceMetricsProvider;
import com.github.workerframework.workermessageprioritization.targetqueue.TunedTargetQueueLengthProvider;
import com.google.common.collect.EvictingQueue;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TunedTargetQueueLengthTest {

    private final double targetQueueLength = 10000;
    private long tunedTargetQueueLength = 0;
    private long tunedTargetQueueLength1 = 0;
    private long tunedTargetQueueLength2 = 0;
    private final int minConsumptionRateHistorySize = 10;
    private final int noMinConsumptionRateHistorySize = 0;
    
    private final String targetQueue1 = "targetQueue1";
    private final String targetQueue2 = "targetQueue2";

    final long minTargetQueueLength = 100;
    final long maxTargetQueueLength = 1000000;
    final long queueProcessingTime = 300; //5 minutes
    final double currentInstances = 2;
    final double maxInstances = 4;

    final TargetQueuePerformanceMetricsProvider targetQueuePerformanceMetricsProvider = mock(TargetQueuePerformanceMetricsProvider.class);

    @Test
    public void getTunedTargetQueueNoOpTest(){

        final TunedTargetQueueLengthProvider targetQueue = new TunedTargetQueueLengthProvider(targetQueuePerformanceMetricsProvider,
                false, minConsumptionRateHistorySize);
        double consumptionRate = 0.5;
        long tunedTargetQueueLength;

        final HashMap<String, Double> mockMetrics = new HashMap<>();

        mockMetrics.put("targetQueueLength",targetQueueLength);
        mockMetrics.put("consumptionRate",consumptionRate);
        mockMetrics.put("currentInstances",currentInstances);
        mockMetrics.put("maxInstances",maxInstances);

        when(targetQueuePerformanceMetricsProvider.getTargetQueuePerformanceMetrics(targetQueue1)).thenReturn(mockMetrics);

        tunedTargetQueueLength = getTunedTargetQueueLength(targetQueue1,targetQueue);

        // NoOpMode on therefore the target queue length will not change
        assertEquals("NoOpMode is on: Target queue length should not have changed.", 10000, tunedTargetQueueLength);
    }

    @Test
    public void getMaxTunedTargetQueueNoOpTest(){

        final TunedTargetQueueLengthProvider targetQueue = new TunedTargetQueueLengthProvider(targetQueuePerformanceMetricsProvider,
                false, minConsumptionRateHistorySize);

        double consumptionRate = 50;
        long tunedTargetQueueLength;

        final HashMap<String, Double> mockMetrics = new HashMap<>();

        mockMetrics.put("targetQueueLength",targetQueueLength);
        mockMetrics.put("consumptionRate",consumptionRate);
        mockMetrics.put("currentInstances",currentInstances);
        mockMetrics.put("maxInstances",maxInstances);

        when(targetQueuePerformanceMetricsProvider.getTargetQueuePerformanceMetrics(targetQueue1)).thenReturn(mockMetrics);

        tunedTargetQueueLength = getTunedTargetQueueLength(targetQueue1,targetQueue);

        // NoOpMode on therefore the target queue length will not change
        assertEquals("NoOpMode is on: Target queue length should not have changed.", 10000, tunedTargetQueueLength);
    }

    @Test
    public void getMinTunedTargetQueueNoOpTest(){

        final TunedTargetQueueLengthProvider targetQueue = new TunedTargetQueueLengthProvider(targetQueuePerformanceMetricsProvider,
                false, minConsumptionRateHistorySize);
        double consumptionRate = 0.05;

        final HashMap<String, Double> mockMetrics = new HashMap<>();

        mockMetrics.put("targetQueueLength",targetQueueLength);
        mockMetrics.put("consumptionRate",consumptionRate);
        mockMetrics.put("currentInstances",currentInstances);
        mockMetrics.put("maxInstances",maxInstances);

        when(targetQueuePerformanceMetricsProvider.getTargetQueuePerformanceMetrics(targetQueue1)).thenReturn(mockMetrics);

        tunedTargetQueueLength = getTunedTargetQueueLength(targetQueue1,targetQueue);

        // NoOpMode on therefore the target queue length will not change
        assertEquals("NoOpMode is on: Target queue length should not have changed.", 10000, tunedTargetQueueLength);
    }

    @Test
    public void getTunedTargetQueueForQueueWithInadequateHistoryAndNoOpModeOffTest(){

        double consumptionRate = 0.5;

        final TunedTargetQueueLengthProvider targetQueue = new TunedTargetQueueLengthProvider(targetQueuePerformanceMetricsProvider,
                false, minConsumptionRateHistorySize);

        final HashMap<String, Double> mockMetrics = new HashMap<>();

        mockMetrics.put("targetQueueLength",targetQueueLength);
        mockMetrics.put("consumptionRate",consumptionRate);
        mockMetrics.put("currentInstances",currentInstances);
        mockMetrics.put("maxInstances",maxInstances);

        when(targetQueuePerformanceMetricsProvider.getTargetQueuePerformanceMetrics(targetQueue1)).thenReturn(mockMetrics);

        tunedTargetQueueLength = getTunedTargetQueueLength(targetQueue1,targetQueue);

        // Not enough history of consumption rate provided therefore target queue length will not change
        assertEquals("Not enough consumption rate history: Target queue length should not have changed.", 10000,
                tunedTargetQueueLength);
    }

    @Test
    public void getMaxTunedTargetQueueForQueueWithInadequateHistoryAndNoOpModeOffTest(){

        final TunedTargetQueueLengthProvider targetQueue = new TunedTargetQueueLengthProvider(targetQueuePerformanceMetricsProvider,
                false, minConsumptionRateHistorySize);
        double consumptionRate = 5000;

        final HashMap<String, Double> mockMetrics = new HashMap<>();

        mockMetrics.put("targetQueueLength",targetQueueLength);
        mockMetrics.put("consumptionRate",consumptionRate);
        mockMetrics.put("currentInstances",currentInstances);
        mockMetrics.put("maxInstances",maxInstances);

        when(targetQueuePerformanceMetricsProvider.getTargetQueuePerformanceMetrics(targetQueue1)).thenReturn(mockMetrics);

        tunedTargetQueueLength = getTunedTargetQueueLength(targetQueue1,targetQueue);
        // Not enough history of consumption rate provided therefore target queue length will not change
        assertEquals("Not enough consumption rate history: Target queue length should not have changed.", 10000,
                tunedTargetQueueLength);
    }

    @Test
    public void getMinTunedTargetQueueForQueueWithInadequateHistoryAndNoOpModeOffTest(){

        final TunedTargetQueueLengthProvider targetQueue = new TunedTargetQueueLengthProvider(targetQueuePerformanceMetricsProvider,
                false, minConsumptionRateHistorySize);
        double consumptionRate = 0.00005;
        long tunedTargetQueueLength;

        final HashMap<String, Double> mockMetrics = new HashMap<>();

        mockMetrics.put("targetQueueLength",targetQueueLength);
        mockMetrics.put("consumptionRate",consumptionRate);
        mockMetrics.put("currentInstances",currentInstances);
        mockMetrics.put("maxInstances",maxInstances);

        when(targetQueuePerformanceMetricsProvider.getTargetQueuePerformanceMetrics(targetQueue1)).thenReturn(mockMetrics);

        tunedTargetQueueLength = getTunedTargetQueueLength(targetQueue1,targetQueue);

        // Not enough history of consumption rate provided therefore target queue length will not change
        assertEquals("Not enough consumption rate history: Target queue length should not have changed.", 10000,
                tunedTargetQueueLength);
    }


    @Test
    public void getTunedTargetQueueForQueueWithNoRequiredHistoryAndNoOpModeOffTest(){

        final TunedTargetQueueLengthProvider targetQueue = new TunedTargetQueueLengthProvider(targetQueuePerformanceMetricsProvider,
                false, noMinConsumptionRateHistorySize);

        double consumptionRate = 0.5;

        final HashMap<String, Double> mockMetrics = new HashMap<>();

        mockMetrics.put("targetQueueLength",targetQueueLength);
        mockMetrics.put("consumptionRate",consumptionRate);
        mockMetrics.put("currentInstances",currentInstances);
        mockMetrics.put("maxInstances",maxInstances);

        when(targetQueuePerformanceMetricsProvider.getTargetQueuePerformanceMetrics(targetQueue1)).thenReturn(mockMetrics);

        tunedTargetQueueLength = getTunedTargetQueueLength(targetQueue1,targetQueue);

        // Not enough history of consumption rate provided therefore target queue length will not change
        assertEquals("Consumption rate history not required: Target queue length should be adjusted.", 300,
                tunedTargetQueueLength);
    }

    @Test
    public void getMaxTunedTargetQueueForQueueWithNoRequiredHistoryAndNoOpModeOffTest(){

        final TunedTargetQueueLengthProvider targetQueue = new TunedTargetQueueLengthProvider(targetQueuePerformanceMetricsProvider,
                false, noMinConsumptionRateHistorySize);

        double consumptionRate = 5000;

        final HashMap<String, Double> mockMetrics = new HashMap<>();

        mockMetrics.put("targetQueueLength",targetQueueLength);
        mockMetrics.put("consumptionRate",consumptionRate);
        mockMetrics.put("currentInstances",currentInstances);
        mockMetrics.put("maxInstances",maxInstances);

        when(targetQueuePerformanceMetricsProvider.getTargetQueuePerformanceMetrics(targetQueue1)).thenReturn(mockMetrics);

        tunedTargetQueueLength = getTunedTargetQueueLength(targetQueue1,targetQueue);

//        tunedTargetQueueLength = getTunedTargetQueueLength(targetQueue1,targetQueue, targetQueueLength, consumptionRate);

        // Not enough history of consumption rate provided therefore target queue length will not change
        assertEquals("Consumption rate history not required: Target queue length should be adjusted.", 1000000,
                tunedTargetQueueLength);
    }

    @Test
    public void getMinTunedTargetQueueForQueueWithNoRequiredHistoryAndNoOpModeOffTest(){

        final TunedTargetQueueLengthProvider targetQueue = new TunedTargetQueueLengthProvider(targetQueuePerformanceMetricsProvider,
                false, noMinConsumptionRateHistorySize);
        double consumptionRate = 0.00005;

        final HashMap<String, Double> mockMetrics = new HashMap<>();

        mockMetrics.put("targetQueueLength",targetQueueLength);
        mockMetrics.put("consumptionRate",consumptionRate);
        mockMetrics.put("currentInstances",currentInstances);
        mockMetrics.put("maxInstances",maxInstances);

        when(targetQueuePerformanceMetricsProvider.getTargetQueuePerformanceMetrics(targetQueue1)).thenReturn(mockMetrics);

        tunedTargetQueueLength = getTunedTargetQueueLength(targetQueue1,targetQueue);

        // Not enough history of consumption rate provided therefore target queue length will not change
        assertEquals("Consumption rate history not required: Target queue length should be adjusted.", 100,
                tunedTargetQueueLength);
    }


    @Test
    public void getTunedTargetQueueForQueueWithAdequateHistoryAndNoOpModeOffTest(){

        double consumptionRate = 0.5;

        final TunedTargetQueueLengthProvider targetQueue = new TunedTargetQueueLengthProvider(targetQueuePerformanceMetricsProvider,
                false, minConsumptionRateHistorySize);

        final HashMap<String, Double> mockMetrics = new HashMap<>();

        mockMetrics.put("targetQueueLength",targetQueueLength);
        mockMetrics.put("consumptionRate",consumptionRate);
        mockMetrics.put("currentInstances",currentInstances);
        mockMetrics.put("maxInstances",maxInstances);

        when(targetQueuePerformanceMetricsProvider.getTargetQueuePerformanceMetrics(targetQueue1)).thenReturn(mockMetrics);

        tunedTargetQueueLength = getTunedTargetQueueLength(targetQueue1,targetQueue);
        mockMetrics.put("targetQueueLength", (double)tunedTargetQueueLength);

        for(int i=0; i < 14 ; i++){
            tunedTargetQueueLength = getTunedTargetQueueLength(targetQueue1,targetQueue);
            mockMetrics.put("targetQueueLength", (double)tunedTargetQueueLength);
        }

//        tunedTargetQueueLength = getTunedTargetQueueLength(targetQueue1, targetQueue, targetQueueLength, consumptionRate);
//        IntStream.range(1, 13).forEach(i -> tunedTargetQueueLength = getTunedTargetQueueLength(targetQueue1, targetQueue,
//                tunedTargetQueueLength, consumptionRate));

        assertEquals("Consumption rate history has been provided: Target queue length should be adjusted.", 300,
                tunedTargetQueueLength);
    }

    @Test
    public void getMaxTunedTargetQueueForQueueWithAdequateHistoryAndNoOpModeOffTest(){

        double consumptionRate = 5000;

        final TunedTargetQueueLengthProvider targetQueue = new TunedTargetQueueLengthProvider(targetQueuePerformanceMetricsProvider,
                false, minConsumptionRateHistorySize);

        final HashMap<String, Double> mockMetrics = new HashMap<>();

        mockMetrics.put("targetQueueLength",targetQueueLength);
        mockMetrics.put("consumptionRate",consumptionRate);
        mockMetrics.put("currentInstances",currentInstances);
        mockMetrics.put("maxInstances",maxInstances);

        when(targetQueuePerformanceMetricsProvider.getTargetQueuePerformanceMetrics(targetQueue1)).thenReturn(mockMetrics);
//
//        when(targetQueuePerformanceMetricsProvider.getTargetQueuePerformanceMetrics(targetQueue1))
//                .thenReturn(new Object());

        tunedTargetQueueLength = getTunedTargetQueueLength(targetQueue1,targetQueue);
        mockMetrics.put("targetQueueLength", (double)tunedTargetQueueLength);

        for(int i=0; i < 14 ; i++){
            tunedTargetQueueLength = getTunedTargetQueueLength(targetQueue1,targetQueue);
            mockMetrics.put("targetQueueLength", (double)tunedTargetQueueLength);
        }

//        IntStream.range(1, 14).forEach(i -> tunedTargetQueueLength = getTunedTargetQueueLength(targetQueue1, targetQueue));

        assertEquals("Consumption rate history has been provided: Target queue length should be adjusted.", 1000000,
                tunedTargetQueueLength);
    }

    @Test
    public void getMinTunedTargetQueueForQueueWithAdequateHistoryAndNoOpModeOffTest(){
//
//        final TunedTargetQueueLengthProvider targetQueue= new TunedTargetQueueLengthProvider(
//                false, minConsumptionRateHistorySize);
        double consumptionRate = 0.00005;

        final TunedTargetQueueLengthProvider targetQueue = new TunedTargetQueueLengthProvider(targetQueuePerformanceMetricsProvider,
                false, minConsumptionRateHistorySize);

        final HashMap<String, Double> mockMetrics = new HashMap<>();

        mockMetrics.put("targetQueueLength",targetQueueLength);
        mockMetrics.put("consumptionRate",consumptionRate);
        mockMetrics.put("currentInstances",currentInstances);
        mockMetrics.put("maxInstances",maxInstances);

        when(targetQueuePerformanceMetricsProvider.getTargetQueuePerformanceMetrics(targetQueue1)).thenReturn(mockMetrics);

        tunedTargetQueueLength = getTunedTargetQueueLength(targetQueue1,targetQueue);
        mockMetrics.put("targetQueueLength", (double)tunedTargetQueueLength);

        for(int i=0; i < 14 ; i++){
            tunedTargetQueueLength = getTunedTargetQueueLength(targetQueue1,targetQueue);
            mockMetrics.put("targetQueueLength", (double)tunedTargetQueueLength);
        }

//        tunedTargetQueueLength = getTunedTargetQueueLength(targetQueue1,targetQueue);
//        IntStream.range(1, 11).forEach(i -> tunedTargetQueueLength = getTunedTargetQueueLength(targetQueue1,targetQueue));

        assertEquals("Consumption rate history has been provided: Target queue length should be adjusted.", 100,
                tunedTargetQueueLength);
    }

    @Test
    public void getErrorWhenMinConsumptionRateHistorySizeLargerThanMaxConsumptionRateHistorySizeTest() {

        final TunedTargetQueueLengthProvider targetQueue = new TunedTargetQueueLengthProvider(targetQueuePerformanceMetricsProvider,
                false, minConsumptionRateHistorySize);

//        final TunedTargetQueueLengthProvider targetQueue = new TunedTargetQueueLengthProvider(
//                false, 101);
        double consumptionRate = 0.00005;

        Throwable exception = assertThrows(IllegalArgumentException.class, () -> {
            throw new IllegalArgumentException("Minimum history required cannot be larger than the maximum history storage size.");
        });
        assertEquals("Minimum history required cannot be larger than the maximum history storage size.", exception.getMessage());
    }

    @Test
    public void getDifferentTunedTargetQueueLengthsForQueuesWithAdequateHistoryAndNoOpModeOffTest(){

        double consumptionRate1 = 0.5;
        double consumptionRate2 = 5;

        final TunedTargetQueueLengthProvider targetQueue = new TunedTargetQueueLengthProvider(targetQueuePerformanceMetricsProvider,
                false, minConsumptionRateHistorySize);

        final HashMap<String, Double> mockMetrics1 = new HashMap<>();
        final HashMap<String, Double> mockMetrics2 = new HashMap<>();

        mockMetrics1.put("targetQueueLength",targetQueueLength);
        mockMetrics1.put("consumptionRate",consumptionRate1);
        mockMetrics1.put("currentInstances",currentInstances);
        mockMetrics1.put("maxInstances",maxInstances);

        when(targetQueuePerformanceMetricsProvider.getTargetQueuePerformanceMetrics(targetQueue1)).thenReturn(mockMetrics1);

        mockMetrics2.put("targetQueueLength",targetQueueLength);
        mockMetrics2.put("consumptionRate",consumptionRate2);
        mockMetrics2.put("currentInstances",currentInstances);
        mockMetrics2.put("maxInstances",maxInstances);

        when(targetQueuePerformanceMetricsProvider.getTargetQueuePerformanceMetrics(targetQueue2)).thenReturn(mockMetrics2);

//        tunedTargetQueueLength1 = getTunedTargetQueueLength(targetQueue1,targetQueue);
//        IntStream.range(1, 11).forEach(i -> tunedTargetQueueLength1 = getTunedTargetQueueLength(targetQueue1, targetQueue));

        tunedTargetQueueLength1 = getTunedTargetQueueLength(targetQueue1,targetQueue);
        mockMetrics1.put("targetQueueLength", (double)tunedTargetQueueLength1);

        for(int i=0; i < 14 ; i++){
            tunedTargetQueueLength1 = getTunedTargetQueueLength(targetQueue1,targetQueue);
            mockMetrics1.put("targetQueueLength", (double)tunedTargetQueueLength1);
        }

        tunedTargetQueueLength2 = getTunedTargetQueueLength(targetQueue2,targetQueue);
        mockMetrics1.put("targetQueueLength", (double)tunedTargetQueueLength2);

        for(int i=0; i < 11 ; i++) {
            tunedTargetQueueLength2 = getTunedTargetQueueLength(targetQueue2, targetQueue);
            mockMetrics1.put("targetQueueLength", (double) tunedTargetQueueLength2);
        }

//        tunedTargetQueueLength2 = getTunedTargetQueueLength(targetQueue2,targetQueue);
//        IntStream.range(1, 11).forEach(i -> tunedTargetQueueLength2 = getTunedTargetQueueLength(targetQueue2,targetQueue));

        assertEquals("Consumption rate history has been provided: Target queue length should be adjusted.", 300,
                tunedTargetQueueLength1);
        assertEquals("Consumption rate history has been provided: Target queue length should be adjusted.", 3000,
                tunedTargetQueueLength2);
    }

    @Test
    public void getTunedTargetQueueForQueueWithAdequateHistoryDifferentConsumptionRatesAndNoOpModeOffTest(){

        final TunedTargetQueueLengthProvider targetQueue = new TunedTargetQueueLengthProvider(targetQueuePerformanceMetricsProvider,
                false, minConsumptionRateHistorySize);

        final HashMap<String, Double> mockMetrics = new HashMap<>();

        mockMetrics.put("targetQueueLength",targetQueueLength);
        mockMetrics.put("consumptionRate", (double) 1);
        mockMetrics.put("currentInstances",currentInstances);
        mockMetrics.put("maxInstances",maxInstances);

        when(targetQueuePerformanceMetricsProvider.getTargetQueuePerformanceMetrics(targetQueue1)).thenReturn(mockMetrics);

        tunedTargetQueueLength = getTunedTargetQueueLength(targetQueue1,targetQueue);
        mockMetrics.put("targetQueueLength", (double)tunedTargetQueueLength);
        mockMetrics.put("consumptionRate", (double) 2);

        for(int i=3; i < 12 ; i++) {
            tunedTargetQueueLength = getTunedTargetQueueLength(targetQueue1, targetQueue);
            mockMetrics.put("targetQueueLength", (double) tunedTargetQueueLength);
            mockMetrics.put("consumptionRate", (double) i);
        }
//
//        tunedTargetQueueLength = getTunedTargetQueueLength(targetQueue1,targetQueue, targetQueueLength, 1);
//        tunedTargetQueueLength = getTunedTargetQueueLength(targetQueue1,targetQueue, tunedTargetQueueLength, 2);
//        tunedTargetQueueLength = getTunedTargetQueueLength(targetQueue1,targetQueue, tunedTargetQueueLength, 3);
//        tunedTargetQueueLength = getTunedTargetQueueLength(targetQueue1,targetQueue, tunedTargetQueueLength, 4);
//        tunedTargetQueueLength = getTunedTargetQueueLength(targetQueue1,targetQueue, tunedTargetQueueLength, 5);
//        tunedTargetQueueLength = getTunedTargetQueueLength(targetQueue1,targetQueue, tunedTargetQueueLength, 6);
//        tunedTargetQueueLength = getTunedTargetQueueLength(targetQueue1,targetQueue, tunedTargetQueueLength, 7);
//        tunedTargetQueueLength = getTunedTargetQueueLength(targetQueue1,targetQueue, tunedTargetQueueLength, 8);
//        tunedTargetQueueLength = getTunedTargetQueueLength(targetQueue1,targetQueue, tunedTargetQueueLength, 9);
//        tunedTargetQueueLength = getTunedTargetQueueLength(targetQueue1,targetQueue, tunedTargetQueueLength, 10);
//        tunedTargetQueueLength = getTunedTargetQueueLength(targetQueue1,targetQueue, tunedTargetQueueLength, 11);

        assertEquals("Consumption rate history has been provided: Target queue length should be adjusted.", 3300,
                tunedTargetQueueLength);
    }

    @Test
    public void getRoundedTunedTargetQueueForQueueWithAdequateHistoryDifferentConsumptionRatesAndNoOpModeOffTest() {
        final TunedTargetQueueLengthProvider targetQueue = new TunedTargetQueueLengthProvider(targetQueuePerformanceMetricsProvider,
                false, minConsumptionRateHistorySize);

        double consumptionRate = 7.6;

        final HashMap<String, Double> mockMetrics = new HashMap<>();

        mockMetrics.put("targetQueueLength",targetQueueLength);
        mockMetrics.put("consumptionRate", consumptionRate);
        mockMetrics.put("currentInstances",currentInstances);
        mockMetrics.put("maxInstances",maxInstances);

        when(targetQueuePerformanceMetricsProvider.getTargetQueuePerformanceMetrics(targetQueue1)).thenReturn(mockMetrics);

        tunedTargetQueueLength = getTunedTargetQueueLength(targetQueue1,targetQueue);
        mockMetrics.put("targetQueueLength", (double)tunedTargetQueueLength);

        for(int i=1; i < 11 ; i++) {
            tunedTargetQueueLength = getTunedTargetQueueLength(targetQueue1, targetQueue);
            mockMetrics.put("targetQueueLength", (double) tunedTargetQueueLength);
        }

//        tunedTargetQueueLength = getTunedTargetQueueLength(targetQueue1,targetQueue, targetQueueLength, consumptionRate);
//        IntStream.range(1, 11).forEach(i -> tunedTargetQueueLength = getTunedTargetQueueLength(targetQueue1,targetQueue,
//                tunedTargetQueueLength,consumptionRate));

        assertEquals("This should return different target queue lengths as the average consumption rate changes (in the range of " +
                        "4550 to 4600). These values should be rounded to 4600 to give a steady target queue length.",
                4600,
                tunedTargetQueueLength);
    }

    private long getTunedTargetQueueLength(final String queueName, final TunedTargetQueueLengthProvider targetQueue) {

        return targetQueue.getTunedTargetQueueLength(queueName, minTargetQueueLength, maxTargetQueueLength, queueProcessingTime);
    }

}
