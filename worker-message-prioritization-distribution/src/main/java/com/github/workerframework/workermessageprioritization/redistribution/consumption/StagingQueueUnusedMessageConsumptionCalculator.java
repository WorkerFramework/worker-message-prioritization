package com.github.workerframework.workermessageprioritization.redistribution.consumption;

import com.github.workerframework.workermessageprioritization.rabbitmq.Queue;
import com.github.workerframework.workermessageprioritization.redistribution.DistributorWorkItem;

import java.math.RoundingMode;
import java.text.DecimalFormat;

public class StagingQueueUnusedMessageConsumptionCalculator {
    final DistributorWorkItem distributorWorkItem;

    public StagingQueueUnusedMessageConsumptionCalculator(DistributorWorkItem distributorWorkItem) {
        this.distributorWorkItem = distributorWorkItem;
    }

    public double calculateStagingQueueUnusedWeight(final long targetQueueCapacity, final double stagingQueueWeight){

        double stagingQueueWeightTotalReduced = 0;
        double unsedStagingQueueWeight = 0;
        long stagingQueueWeightFromSettingService = 1;

        final double targetQueueCapacityPerWeight = targetQueueCapacity / stagingQueueWeight;

        for (final Queue stagingQueue : distributorWorkItem.getStagingQueues()) {
            final double numMessagesInStagingQueue = stagingQueue.getMessages();

            // Calculate the capacity provided to each queue based on it's weight
            final double queueValueGiven = targetQueueCapacityPerWeight * stagingQueueWeightFromSettingService;

            // If a staging queue has fewer messages than the available capacity, this is unusedWeight.
            // This is calculated by dividing the number of messages in the staging queue by the value given. This is then taken away
            // from the original weight of the staging queue. eg: if queue has 50 messages but is offered capacity of 200 and has a
            // weight of 2. Then 50/200 = 0.25. 2 - 0.25 = 1.75.
            // There is an unused weight of 1.75 in this case.
            if (numMessagesInStagingQueue < queueValueGiven) {
                unsedStagingQueueWeight += (stagingQueueWeightFromSettingService - (numMessagesInStagingQueue / queueValueGiven));
                // Keep trac of the total weights that are not used to their potential. In this case weight of 2 is added, as that full
                // potential was not used.
                stagingQueueWeightTotalReduced += stagingQueueWeightFromSettingService;
            }
        }

        // Rounding values to ensure they are no longer than 3dp.
        DecimalFormat df = new DecimalFormat("#.###");
        df.setRoundingMode(RoundingMode.FLOOR);

        // If all staging queues are smaller than the capacity available to them, return zero as unused weight is useless regardless.
        // Also, if all staging queues are larger than the capacity available to them (in other works there is no unused staging queue
        // weight) then also return zero.
        if((stagingQueueWeight - stagingQueueWeightTotalReduced) == 0 || unsedStagingQueueWeight == 0){
            return 0;
        }else {
            // Unused weight returned will be the total unused weight, divided by the difference between the total staging
            // queue weights, and the sum of all staging queue weights that had a length less than the available capacity.
            // This means that the leftover weight is split evenly among the staging queues that have a length greater than the
            // capacity.
            // Although this weight addition will be added to all staging queues, it will only be used by those with the larger
            // capacity, therefore leftovers will be split evenly in the end.
            final double unusedWeightAddition = (unsedStagingQueueWeight / (stagingQueueWeight - stagingQueueWeightTotalReduced));
            return Double.parseDouble(df.format(unusedWeightAddition));
        }
    }
}
