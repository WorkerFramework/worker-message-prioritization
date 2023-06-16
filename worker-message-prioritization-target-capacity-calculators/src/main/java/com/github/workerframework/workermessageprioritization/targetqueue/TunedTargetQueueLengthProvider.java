package com.github.workerframework.workermessageprioritization.targetqueue;

import com.google.common.collect.EvictingQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;

public class TunedTargetQueueLengthProvider {

    private final TargetQueuePerformanceMetricsProvider targetQueuePerformanceMetricsProvider;
    private final boolean noOpMode;

    private final int minConsumptionRateHistorySize;

    private static final Logger LOGGER = LoggerFactory.getLogger(TunedTargetQueueLengthProvider.class);

    private final int maximumConsumptionRateHistorySize = 100;

    private final int roundingMultiple = 100;

    private final HashMap<String, EvictingQueue<Double>> consumptionRateHistoryMap = new HashMap<>();

    public TunedTargetQueueLengthProvider (final TargetQueuePerformanceMetricsProvider targetQueuePerformanceMetricsProvider,
                                           final boolean noOpMode, final int minConsumptionRateHistorySize){
        this.targetQueuePerformanceMetricsProvider = targetQueuePerformanceMetricsProvider;
        this.noOpMode = noOpMode;
        this.minConsumptionRateHistorySize = minConsumptionRateHistorySize;
    }

//    public TunedTargetQueueLengthProvider(final boolean noOpMode, final int minConsumptionRateHistorySize){
//        this.noOpMode = noOpMode;
//        this.minConsumptionRateHistorySize = minConsumptionRateHistorySize;
//    }

    public final long getTunedTargetQueueLength(final String targetQueueName,
                                                final long minTargetQueueLength, final long maxTargetQueueLength,
                                                final double queueProcessingTimeGoalSeconds){

        final HashMap<String, Double> performanceMetrics = targetQueuePerformanceMetricsProvider.getTargetQueuePerformanceMetrics(targetQueueName);

        final double metricsTargetQueueLength = performanceMetrics.get("targetQueueLength");
        final double metricsConsumptionRate = performanceMetrics.get("consumptionRate");
        final double metricsCurrentInstances = performanceMetrics.get("currentInstances");
        final double metricsMaxInstances = performanceMetrics.get("maxInstances");

        checkMaxConsumptionRateHistorySize(maximumConsumptionRateHistorySize, minConsumptionRateHistorySize);

        //        0.5/s 30 messages, 60 seconds
        logNoOpModeInformation();
        logCurrentTargetQueueInformation((long)metricsTargetQueueLength, targetQueueName);
        logTargetQueueRoundingInformation(roundingMultiple, minTargetQueueLength);

//        final Object targetQueuePerformanceMetrics =
//                targetQueuePerformanceMetricsProvider.getTargetQueuePerformanceMetrics(targetQueueName);

        final double theoreticalConsumptionRate = calculateTheoreticalConsumptionRate(metricsConsumptionRate, metricsCurrentInstances,
                metricsMaxInstances);

        EvictingQueue<Double> theoreticalConsumptionRateHistory = getTheoreticalConsumptionRateHistory(targetQueueName);
        theoreticalConsumptionRateHistory.add(theoreticalConsumptionRate);
        consumptionRateHistoryMap.put(targetQueueName, theoreticalConsumptionRateHistory);

        long tunedTargetQueue = calculateTunedTargetQueue(theoreticalConsumptionRateHistory, queueProcessingTimeGoalSeconds);

        final int consumptionRateHistorySize = theoreticalConsumptionRateHistory.size();
        logConsumptionRateHistoryInformation(consumptionRateHistorySize);

        LOGGER.info("Considering the average theoretical consumption rate of this worker, the target queue length should be: " +
                tunedTargetQueue);

        final RoundTargetQueueLength roundTargetQueueLength = new RoundTargetQueueLength(roundingMultiple);
        long roundedTargetQueue = roundTargetQueueLength.getRoundedTargetQueueLength(tunedTargetQueue);

        roundedTargetQueue = checkMaxTargetQueueLength(roundedTargetQueue, maxTargetQueueLength);
        roundedTargetQueue = checkMinTargetQueueLength(roundedTargetQueue, minTargetQueueLength);

        return determineFinalTargetQueueLength(consumptionRateHistorySize, (long) metricsTargetQueueLength,
                roundedTargetQueue);
    }

    private void checkMaxConsumptionRateHistorySize(final int maximumConsumptionRateHistorySize, final long minConsumptionRateHistorySize)
            throws IllegalArgumentException {
        if (maximumConsumptionRateHistorySize < minConsumptionRateHistorySize) {
            LOGGER.error("Minimum history required cannot be larger than the maximum history storage size.");
            throw new IllegalArgumentException();
        }
    }

    private double calculateTheoreticalConsumptionRate(final double currentConsumptionRate, final double currentInstances,
                                                       final double maxInstances){
        return (currentConsumptionRate / currentInstances) * maxInstances;
    }

    private double calculateAverageTheoreticalConsumptionRate(final EvictingQueue<Double> evictingQueue){
        return evictingQueue.stream().mapToDouble(d -> d).average().getAsDouble();
    }

    private long calculateTunedTargetQueue(final EvictingQueue<Double> consumptionRateHistory, final double queueProcessingTimeGoalSeconds){
        final double averageTheoreticalConsumptionRate = calculateAverageTheoreticalConsumptionRate(consumptionRateHistory);
        final double suggestedTargetQueueLength = averageTheoreticalConsumptionRate * queueProcessingTimeGoalSeconds;
        return (long) suggestedTargetQueueLength;
    }
    
    private long checkMaxTargetQueueLength(final long tunedTargetQueue, final long maxTargetQueueLength){
        if (tunedTargetQueue > maxTargetQueueLength) {
            LOGGER.info(tunedTargetQueue+ " exceeds the maximum length that the queue can be set to. Therefore the maximum length: "
                    + maxTargetQueueLength + " should be set.");
            return maxTargetQueueLength;
        }else {
            return tunedTargetQueue;
        }
    }    
    
    private long checkMinTargetQueueLength(final long tunedTargetQueue, final long minTargetQueueLength){
        if(tunedTargetQueue < minTargetQueueLength){
            LOGGER.info(tunedTargetQueue+ " is less than the minimum length that the queue can be set to. Therefore the minimum length: "
                    + minTargetQueueLength + " should be set.");
            return minTargetQueueLength;
        }else {
            return tunedTargetQueue;
        }
    }

    private void logNoOpModeInformation(){
        if (noOpMode){
            LOGGER.info("noOpMode is on. The queue length will not be altered. Recommendations of how the queue length could " +
                    "be altered based on consumption rate will be logged.");
        }else{
            LOGGER.info("noOpMode is off. Provided there is sufficient consumption rate history of the worker, the target queue length " +
                    "will be altered.");
        }
    }

    private void logCurrentTargetQueueInformation(final long targetQueueLength, final String targetQueueName){
        LOGGER.info("Current target queue length for " + targetQueueName + ": " + targetQueueLength);
    }

    private void logTargetQueueRoundingInformation(final long roundingMultiple, final long minTargetQueueLength){
        LOGGER.info("RoundingMultiple value has been set to: " + roundingMultiple + ". This means any suggested target queues that are " +
                "not a multiple of " + roundingMultiple + ", will be rounded to the nearest multiple. In the case that the target " +
                "queue length is rounded to 0, The target queue length will be set to the minimum target queue length size: " +
                minTargetQueueLength);
    }

    private void logConsumptionRateHistoryInformation(final int consumptionRateHistorySize){
        if(consumptionRateHistorySize >= minConsumptionRateHistorySize && !noOpMode){
            LOGGER.info("There is consumption rate history from the last " + consumptionRateHistorySize +" runs of this worker " +
                    "present. An average of these rates will determine the new target queue length. If different to the current queue " +
                    "length, the following suggestions will be implemented and the target queue length adjusted.");
        }else if (!noOpMode){
            LOGGER.info("There is not enough history to tune the target queue length accurately. The following logs are " +
                    "recommendations. The target queue will not be adjusted until more history is present.");
        }
    }

    private EvictingQueue<Double> getTheoreticalConsumptionRateHistory(final String targetQueueName){
        if(consumptionRateHistoryMap.containsKey(targetQueueName)){
            return consumptionRateHistoryMap.get(targetQueueName);
        }else {
            return EvictingQueue.create(maximumConsumptionRateHistorySize); //Make the hundred configurable
        }
    }

    private long determineFinalTargetQueueLength(final int consumptionRateHistorySize, final long targetQueueLength,
                                                 final long tunedTargetQueue){

        if(noOpMode) {
            LOGGER.info("NoOpMode True - Target queue length has not been adjusted.");
            return targetQueueLength;
        }

        if(consumptionRateHistorySize < minConsumptionRateHistorySize ) {
            LOGGER.info("Not enough ConsumptionRateHistory to make an adjustment.");
            return targetQueueLength;
        }

        if(targetQueueLength == tunedTargetQueue){
            LOGGER.info("Target queue is already set to optimum length: " + targetQueueLength + ". No action required.");
            return targetQueueLength;
        }else{
            LOGGER.info("Target queue length has been adjusted to: " + tunedTargetQueue);
            return tunedTargetQueue;
        }
    }
}
