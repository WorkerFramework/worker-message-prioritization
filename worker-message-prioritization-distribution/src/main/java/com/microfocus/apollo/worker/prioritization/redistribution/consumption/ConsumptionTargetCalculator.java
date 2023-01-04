/*
 * Copyright 2022-2022 Micro Focus or one of its affiliates.
 *
 * The only warranties for products and services of Micro Focus and its
 * affiliates and licensors ("Micro Focus") are set forth in the express
 * warranty statements accompanying such products and services. Nothing
 * herein should be construed as constituting an additional warranty.
 * Micro Focus shall not be liable for technical or editorial errors or
 * omissions contained herein. The information contained herein is subject
 * to change without notice.
 *
 * Contains Confidential Information. Except as specifically indicated
 * otherwise, a valid license is required for possession, use or copying.
 * Consistent with FAR 12.211 and 12.212, Commercial Computer Software,
 * Computer Software Documentation, and Technical Data for Commercial
 * Items are licensed to the U.S. Government under vendor's standard
 * commercial license.
 */
package com.microfocus.apollo.worker.prioritization.redistribution.consumption;

import com.microfocus.apollo.worker.prioritization.rabbitmq.Queue;
import com.microfocus.apollo.worker.prioritization.redistribution.DistributorWorkItem;

import java.util.Map;

public interface ConsumptionTargetCalculator {
    /**
     * Calculate how many messsages should be consumed from the staging queues
     * @param distributorWorkItem The target queue and the staging queues containing messages to be sent to the target 
     *                            queue.
     * @return A map containing the staging queues for a target queue and how many messages to consume from each 
     * staging queue
     */
    Map<Queue, Long> calculateConsumptionTargets(final DistributorWorkItem distributorWorkItem);
    
}
