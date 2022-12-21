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
package com.microfocus.apollo.worker.prioritization.redistribution;

import com.microfocus.apollo.worker.prioritization.rabbitmq.Queue;
import com.microfocus.apollo.worker.prioritization.rabbitmq.QueuesApi;
import com.microfocus.apollo.worker.prioritization.rabbitmq.RabbitManagementApi;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public abstract class MessageDistributor {

    public static final String LOAD_BALANCED_INDICATOR = "»";

    private final RabbitManagementApi<QueuesApi> queuesApi;

    public MessageDistributor(final RabbitManagementApi<QueuesApi> queuesApi) {
        this.queuesApi = queuesApi;
    }
    
    protected Set<DistributorWorkItem> getDistributorTargets() {
        final List<Queue> queues = queuesApi.getApi().getQueues();
        final Set<DistributorWorkItem> distributorWorkItems = new HashSet<>();
        
        for(final Queue targetQueue: queues.stream().filter(q -> !q.getName().contains(LOAD_BALANCED_INDICATOR))
                .collect(Collectors.toSet())) {
            
            final Set<Queue> stagingQueues = queues.stream()
                    .filter(q -> q.getName().startsWith(targetQueue + LOAD_BALANCED_INDICATOR))
                    .collect(Collectors.toSet());
            
            if(stagingQueues.isEmpty()) {
                continue;
            }
            
            distributorWorkItems.add(new DistributorWorkItem(targetQueue, stagingQueues));
            
        }
        return distributorWorkItems;
    }
}
