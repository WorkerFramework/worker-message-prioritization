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
import com.github.workerframework.workermessageprioritization.redistribution.DistributorWorkItem;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.HashMap;
import java.util.Map;

public class StagingQueueUnusedWeightCalculator {

    public StagingQueueUnusedWeightCalculator() {
    }

    public double calculateStagingQueueUnusedWeight(final DistributorWorkItem distributorWorkItem,
                                                    final long targetQueueCapacity,
                                                    final double totalStagingQueueWeight,
                                                    Map<String, Double> stagingQueueWeightMap){

        boolean unusedWeightAvailable = true;
        double weightAdditionPerQueue = 0;
        double totalUnusedWeight = 0;
        double previousUnusedWeight = 0;
        double totalStagingQueueWeightReduced = 0;
        boolean firstScanComplete = false;

        final double targetQueueCapacityPerWeight = targetQueueCapacity / totalStagingQueueWeight;

        // Will continue in this loop until there is no unused weight potential found.
        while (unusedWeightAvailable) {

            final Map<String, Double> weightValues = getStagingQueueStats(distributorWorkItem, targetQueueCapacityPerWeight,
                    weightAdditionPerQueue, previousUnusedWeight, firstScanComplete, stagingQueueWeightMap);

            // Update the previous unused weight to store the old value in order to calculate the weight unused.
            previousUnusedWeight = totalUnusedWeight;

            final double unusedStagingQueueWeight = weightValues.get("UnusedStagingQueueWeight");
            final double usedStagingQueueWeight = weightValues.get("UsedStagingQueueWeight");
            final double stagingQueueWeightReduced = weightValues.get("StagingQueueWeightTotalReduced");

            // If all staging queues are smaller than the capacity available to them, return zero as unused weight is useless regardless.
            // Also, if all staging queues are larger than the capacity available to them (in other words there is no unused staging queue
            // weight) then also return zero.
            // Once no unused weight is found, exit loop.
            if((totalStagingQueueWeight - stagingQueueWeightReduced) == 0 || unusedStagingQueueWeight == 0){
                unusedWeightAvailable = false;
            }

            // If this is the first scan of the staging queues, add the unused weight to find the total unused.
            if(!firstScanComplete){
                totalUnusedWeight += unusedStagingQueueWeight;
            } else {
                // If the first scan of the staging queues has already happened then we need to remove the weight used by queues that
                // could not use all the additional weight given.
                totalUnusedWeight -= usedStagingQueueWeight;
            }

            // Calculates the total weight of staging queues that left behind unused weight.
            totalStagingQueueWeightReduced += stagingQueueWeightReduced;

            // Calculate the weight to be added to each queue.
            weightAdditionPerQueue = (totalUnusedWeight / (totalStagingQueueWeight - totalStagingQueueWeightReduced));

            firstScanComplete = true;

        }

        if(totalUnusedWeight == 0){
            return 0;
        }
        // If the totalStagingQueueWeight == totalStagingQueueWeightReduced, this means that the max of each queue is still
        // not enough to fill the capacity of the target queue. This means we just want to set each queue to have a
        // capacity of their length. Because in the FastLaneConsumptionTargetCalculator we set
        // actualNumberOfMessagesToConsumeFromStagingQueue to the minimum. This means returning the totalStagingQueueWeight
        // at this point will return the minimum of the staging queue or the staging queue * totalStagingQueueWeight.
        // This means that in this scenario the full length of the
        if(totalStagingQueueWeight == totalStagingQueueWeightReduced){
            return totalStagingQueueWeight;
        }
        else{
            // Unused weight returned will be the total unused weight, divided by the difference between the total staging
            // queue weights, and the sum of all staging queue weights that had a length less than the available capacity.
            // This means that the leftover weight is split evenly among the staging queues that have a length greater than the
            // capacity.
            // Although this weight addition will be added to all staging queues, it will only be used by those with the larger
            // capacity, therefore leftovers will be split evenly in the end.
            return round(totalUnusedWeight / (totalStagingQueueWeight - totalStagingQueueWeightReduced), 3);
        }
    }

    private Map<String, Double> getStagingQueueStats(final DistributorWorkItem distributorWorkItem,
                                                     double targetQueueCapacityPerWeight,
                                                     double weightAdditionPerQueue,
                                                     double previousWeightAddition,
                                                     boolean firstScanComplete,
                                                     Map<String, Double> stagingQueueWeightMap){

        double stagingQueueWeightTotalReduced = 0;
        double unusedStagingQueueWeight = 0;
        double usedStagingQueueWeight = 0;

        for (final Queue stagingQueue : distributorWorkItem.getStagingQueues()) {
            final double numMessagesInStagingQueue = stagingQueue.getMessages();

            // Calculate the capacity provided to each queue based on its weight
            final double capacityPerQueue =
                    targetQueueCapacityPerWeight * (stagingQueueWeightMap.get(stagingQueue.getName()) + weightAdditionPerQueue);

            //Store the previous capacity per queue to
            final double previousCapacityPerQueue =
                    targetQueueCapacityPerWeight * (stagingQueueWeightMap.get(stagingQueue.getName()) + previousWeightAddition);

            // If the first scan for unused weight is complete, need to consider queues that
            // had more or equal messages to the original capacity,
            // however less than the capacity provided with the extra weight added.
            if(firstScanComplete){
                if (numMessagesInStagingQueue < capacityPerQueue && numMessagesInStagingQueue >= previousCapacityPerQueue) {
                    double usedWeight = round((numMessagesInStagingQueue / previousCapacityPerQueue)
                                    - stagingQueueWeightMap.get(stagingQueue.getName()), 4);
                    usedStagingQueueWeight += usedWeight;
                    // Keep track of the total weights that are not used to their potential.
                    // In this case weight of 2 is added, as that full potential was not used.
                    stagingQueueWeightTotalReduced += stagingQueueWeightMap.get(stagingQueue.getName());
                }
            // For the first scan for unused weight, only need to consider queue lengths that are
            // less than the capacity of the target queue provided to each staging queue.
            }else{
                if (numMessagesInStagingQueue < capacityPerQueue) {
                    // If a staging queue has fewer messages than the available capacity, this is unusedWeight.
                    // This is calculated by dividing the number of messages in the staging queue by the value given.
                    // This is then taken away from the original weight of the staging queue. eg:
                    // if queue has 50 messages but is offered capacity of 200 and has a weight of 2.
                    // Then 50/200 = 0.25. 2 - 0.25 = 1.75.
                    // In this case there is an unused weight of 1.75.
                    unusedStagingQueueWeight +=
                            round((stagingQueueWeightMap.get(stagingQueue.getName()) - (numMessagesInStagingQueue / capacityPerQueue)), 4);
                    // Need to keep track of the total weights that are not used to their potential.
                    // In this case weight of 2 is added, as that full potential was not used.
                    // This is required to divide the unused weight only by queues that did not have unused weight
                    // (this value will be used to calculate that difference)
                    stagingQueueWeightTotalReduced += stagingQueueWeightMap.get(stagingQueue.getName());
                }
            }
        }
        Map<String, Double> weightValues = new HashMap<>();
        weightValues.put("UnusedStagingQueueWeight", unusedStagingQueueWeight);
        weightValues.put("UsedStagingQueueWeight", usedStagingQueueWeight);
        weightValues.put("StagingQueueWeightTotalReduced", stagingQueueWeightTotalReduced);
        return weightValues;
    }

    // BigDecimal used to more accurately round the double values.
    private double round(double value, int places){
        if (places < 0){
            throw new IllegalArgumentException();
        }

        BigDecimal bd = BigDecimal.valueOf(value);
        bd = bd.setScale(places, RoundingMode.CEILING);
        return bd.doubleValue();
    }
}
