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
package com.github.workerframework.workermessageprioritization.targetqueue;

import com.google.common.collect.EvictingQueue;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;

@SuppressWarnings("UnstableApiUsage")
public class HistoricalConsumptionRateManager {

    private static final Logger TUNED_TARGET_LOGGER = LoggerFactory.getLogger("TUNED_TARGET");
    private final int maximumConsumptionRateHistorySize;
    private final int minimumHistorySize;
    private final HashMap<String, EvictingQueue<Double>> consumptionRateHistoryMap = new HashMap<>();

    @Inject
    public HistoricalConsumptionRateManager(
            @Named("MaxConsumptionRateHistorySize") final int maximumConsumptionRateHistorySize,
            @Named("MinConsumptionRateHistorySize")final int minimumHistorySize) throws IllegalArgumentException {

        if (maximumConsumptionRateHistorySize < minimumHistorySize) {
            throw new IllegalArgumentException(
                    "Minimum history required cannot be larger than the maximum history storage size.");
        }

        this.maximumConsumptionRateHistorySize = maximumConsumptionRateHistorySize;
        this.minimumHistorySize = minimumHistorySize;
    }

    public double recordCurrentConsumptionRateHistoryAndGetAverage(final String targetQueueName, 
                                                                   final double theoreticalConsumptionRate,
                                                                   final double messageBytesReady) {

        final EvictingQueue<Double> theoreticalConsumptionRateHistory = consumptionRateHistoryMap
                .computeIfAbsent(targetQueueName, s -> EvictingQueue.create(maximumConsumptionRateHistorySize));

        if(messageBytesReady != 0 || theoreticalConsumptionRateHistory.isEmpty()) {

            if(theoreticalConsumptionRateHistory.isEmpty()){
                TUNED_TARGET_LOGGER.debug(
                        "History rate for {} is empty therefore this consumption rate will be recorded.",
                        targetQueueName);
            }

            if (messageBytesReady != 0) {
                TUNED_TARGET_LOGGER.debug(
                        "There are message bytes ready for: {}, therefore this consumption rate will be recorded.",
                        targetQueueName);
            }

            theoreticalConsumptionRateHistory.add(theoreticalConsumptionRate);
        }

        return theoreticalConsumptionRateHistory.stream().mapToDouble(d -> d).average()
                .orElseThrow(IllegalStateException::new);
    }

    public boolean isSufficientHistoryAvailable(final String queueName) throws IllegalArgumentException {
        if(minimumHistorySize == 0){
            return true;
        }
        if (!consumptionRateHistoryMap.containsKey(queueName)) {
            throw new IllegalArgumentException("Queue with this name not found.");
        } else {
            final boolean isSufficientHistory = consumptionRateHistoryMap.get(queueName).size() >= minimumHistorySize;

            if (isSufficientHistory) {
                TUNED_TARGET_LOGGER.debug(
                        "Consumption rate history from the last {} runs of this worker available. An average of these" +
                        " rates will determine the new target queue length. If different to the current queue length," +
                                " the following " +
                        "suggestions will be implemented and the target queue length adjusted.",
                        consumptionRateHistoryMap.get(queueName).size());
            } else {
                TUNED_TARGET_LOGGER.debug("Consumption rate history from the last {} runs of this worker available. " +
                        "There is not enough history to tune the target queue length accurately. The following logs " +
                        "are recommendations. The target queue will not be adjusted until more history is present.",
                        consumptionRateHistoryMap.get(queueName).size());
            }
            return isSufficientHistory;
        }
    }
}
