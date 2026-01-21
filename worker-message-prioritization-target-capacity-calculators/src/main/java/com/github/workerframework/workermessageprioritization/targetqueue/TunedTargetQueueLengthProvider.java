/*
 * Copyright 2022-2026 Open Text.
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
package com.github.workerframework.workermessageprioritization.targetqueue;

import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TunedTargetQueueLengthProvider {

    private static final Logger TUNED_TARGET_LOGGER = LoggerFactory.getLogger("TUNED_TARGET");
    private final QueueInformationProvider queueInformationProvider;
    private final boolean enableTargetQueueLengthTuning;
    private final double queueProcessingTimeGoalSeconds;
    private final HistoricalConsumptionRateManager historicalConsumptionRateManager;
    private final TargetQueueLengthRounder targetQueueLengthRounder;
    private final long minTargetQueueLength;
    private final long maxTargetQueueLength;

    @Inject
    public TunedTargetQueueLengthProvider (final QueueInformationProvider queueInformationProvider,
                                           final HistoricalConsumptionRateManager historicalConsumptionRateManager,
                                           final TargetQueueLengthRounder targetQueueLengthRounder,
                                           @Named("MinTargetQueueLength") final long minTargetQueueLength,
                                           @Named("MaxTargetQueueLength") final long maxTargetQueueLength,
                                           @Named("EnableTargetQueueLengthTuning") final boolean enableTargetQueueLengthTuning,
                                           @Named("QueueProcessingTimeGoalSeconds") 
                                               final double queueProcessingTimeGoalSeconds) {
        this.queueInformationProvider = queueInformationProvider;
        this.historicalConsumptionRateManager = historicalConsumptionRateManager;
        this.targetQueueLengthRounder = targetQueueLengthRounder;
        this.minTargetQueueLength = minTargetQueueLength;
        this.maxTargetQueueLength = maxTargetQueueLength;
        this.enableTargetQueueLengthTuning = enableTargetQueueLengthTuning;
        this.queueProcessingTimeGoalSeconds = queueProcessingTimeGoalSeconds;
    }

    public long getTunedTargetQueueLength(final String targetQueueName, final TargetQueueSettings targetQueueSettings){

        final double consumptionRate = queueInformationProvider.getConsumptionRate(targetQueueName);

        TUNED_TARGET_LOGGER.debug("Current consumption rate of {} is: {}", targetQueueName, consumptionRate);

        final double messageBytesReady = queueInformationProvider.getMessageBytesReady(targetQueueName);

        final double theoreticalConsumptionRate = calculateCurrentTheoreticalConsumptionRate(consumptionRate,
                targetQueueSettings.getCurrentInstances(), targetQueueSettings.getMaxInstances());

        final double averageHistoricalConsumptionRate =
                historicalConsumptionRateManager.recordCurrentConsumptionRateHistoryAndGetAverage(targetQueueName,
                        theoreticalConsumptionRate, messageBytesReady);

        final long tunedTargetQueue = calculateTunedTargetQueue(averageHistoricalConsumptionRate);

        final long roundedTargetQueueLength = roundAndCheckTargetQueue(tunedTargetQueue);

        return determineFinalTargetQueueLength(targetQueueName, targetQueueSettings.getCurrentMaxLength(), 
                roundedTargetQueueLength);
    }

    private double calculateCurrentTheoreticalConsumptionRate(final double currentConsumptionRate, 
                                                              final double currentInstances,
                                                              final double maxInstances){
        if(currentInstances == 0D){
            return currentConsumptionRate * maxInstances;
        }else {
            return (currentConsumptionRate / currentInstances) * maxInstances;
        }
    }

    private long calculateTunedTargetQueue(final double averageTheoreticalConsumptionRate){
        final double suggestedTargetQueueLength = averageTheoreticalConsumptionRate * queueProcessingTimeGoalSeconds;
        return (long) suggestedTargetQueueLength;
    }

    private long roundAndCheckTargetQueue(final long tunedTargetQueue) {
        final long roundedTargetQueueLength = targetQueueLengthRounder.getRoundedTargetQueueLength(tunedTargetQueue);
        TUNED_TARGET_LOGGER.debug(
                "In the case that the target queue length is rounded below the minimum target queue length or above " +
                "the maximum target queue length. " +
                "Target queue length will be set to that minimum or maximum value respectively.");

        if (roundedTargetQueueLength > maxTargetQueueLength) {
            TUNED_TARGET_LOGGER.debug(
                    "Rounded queue length: {} exceeds the maximum length that the queue can be set to. " +
                    "Therefore the maximum length: {} should be set.", roundedTargetQueueLength, maxTargetQueueLength);
            return maxTargetQueueLength;
        }

        if (roundedTargetQueueLength < minTargetQueueLength) {
            TUNED_TARGET_LOGGER.debug(
                    "Rounded queue length: {} is less than the minimum length that the queue can be set to. Therefore" +
                    " the minimum length: {} should be set.", roundedTargetQueueLength, minTargetQueueLength);
            return minTargetQueueLength;
        } else {
            return roundedTargetQueueLength;
        }
    }

    private long determineFinalTargetQueueLength(final String targetQueueName, final long targetQueueLength, 
                                                 final long tunedTargetQueueLength){

        TUNED_TARGET_LOGGER.info("Original target queue length of {}: {}", targetQueueName, targetQueueLength);

        TUNED_TARGET_LOGGER.info("Recommended tuned target queue length of {} is: {}", targetQueueName, 
                tunedTargetQueueLength);

        if(!enableTargetQueueLengthTuning) {
            TUNED_TARGET_LOGGER.info("Tuning is disabled - Target queue length of {} has not been adjusted.",
                    targetQueueName);
            return targetQueueLength;
        }

        if(!historicalConsumptionRateManager.isSufficientHistoryAvailable(targetQueueName)) {
            TUNED_TARGET_LOGGER.info(
                    "Not enough ConsumptionRateHistory to make an adjustment to {} target queue length.", 
                    targetQueueName);
            return targetQueueLength;
        }

        if(targetQueueLength == tunedTargetQueueLength){
            TUNED_TARGET_LOGGER.info(
                    "Target queue length of {} is already set to optimum length: {}. No action required.",
                    targetQueueName, targetQueueLength);
            return targetQueueLength;
        }else{
            TUNED_TARGET_LOGGER.info("Target queue length of {} has been adjusted to: {}", targetQueueName, 
                    tunedTargetQueueLength);
            return tunedTargetQueueLength;
        }
    }
}
