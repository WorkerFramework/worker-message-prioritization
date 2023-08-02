package com.github.workerframework.workermessageprioritization.redistribution.consumption;

import com.github.workerframework.workermessageprioritization.rabbitmq.Queue;
import com.github.workerframework.workermessageprioritization.redistribution.DistributorWorkItem;

import java.util.HashMap;
import java.util.Map;

public class StagingQueueWeightCalculator {
    long stagingQueueWeight = 0;
    final DistributorWorkItem distributorWorkItem;

    public StagingQueueWeightCalculator(DistributorWorkItem distributorWorkItem) {
        this.distributorWorkItem = distributorWorkItem;
    }

    public double calculateTotalStagingQueueWeight(){
        for (final Queue stagingQueue : distributorWorkItem.getStagingQueues()) {
            stagingQueueWeight += 1;
            //get these from the settings service.
        }
        return stagingQueueWeight;
    }

    public Map<Queue, Double> getStagingQueueWeights(){

        final Map<Queue, Double> stagingQueueWeights = new HashMap<>();

        for (final Queue stagingQueue : distributorWorkItem.getStagingQueues()) {
            //call to the settings service to get the weight of a queue
            stagingQueueWeights.put(stagingQueue, 1D);
        }

        return stagingQueueWeights;
    }
}
