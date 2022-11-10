package com.microfocus.apollo.worker.prioritization.redistribution;

import com.microfocus.apollo.worker.prioritization.management.Component;
import com.microfocus.apollo.worker.prioritization.management.Queue;
import com.microfocus.apollo.worker.prioritization.management.QueuesApi;
import com.microfocus.apollo.worker.prioritization.management.RabbitManagementApi;
import com.microfocus.apollo.worker.prioritization.management.RetrievedShovel;
import com.microfocus.apollo.worker.prioritization.management.Shovel;
import com.microfocus.apollo.worker.prioritization.management.ShovelsApi;
import com.microfocus.apollo.worker.prioritization.rerouting.MessageRouter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class ShovelDistributor {

    private static final Logger LOGGER = LoggerFactory.getLogger(ShovelDistributor.class);
    private final RabbitManagementApi<QueuesApi> queuesApi;
    private final RabbitManagementApi<ShovelsApi> shovelsApi;
    private final long targetQueueMessageLimit;

    public ShovelDistributor(final RabbitManagementApi<QueuesApi> queuesApi,
                                        final RabbitManagementApi<ShovelsApi> shovelsApi, 
                                        final long targetQueueMessageLimit) {

        this.queuesApi = queuesApi;
        this.shovelsApi = shovelsApi;
        this.targetQueueMessageLimit = targetQueueMessageLimit;
    }
    
    public void run() {
        
        final List<Queue> queues = queuesApi.getApi().getQueues();
        final Set<Queue> targetQueues = getMesssageTargetsQueues(queues);
        
        final List<RetrievedShovel> retrievedShovels = shovelsApi.getApi().getShovels();
        
        for(final Queue targetQueue: targetQueues) {
            
            final Set<Queue> sourceQueues = getSourceQueues(targetQueue, queues);

            final long lastKnownTargetQueueLength = targetQueue.getMessages();

            final long totalKnownPendingMessages = 
                    sourceQueues.stream().map(Queue::getMessages).mapToLong(Long::longValue).sum();

            final long consumptionTarget = targetQueueMessageLimit - lastKnownTargetQueueLength;
            final long sourceQueueConsumptionTarget;
            if(sourceQueues.isEmpty()) {
                sourceQueueConsumptionTarget = 0;
            }
            else {
                sourceQueueConsumptionTarget = (long) Math.ceil((double)consumptionTarget / sourceQueues.size());
            }

            LOGGER.info("TargetQueue {}, {} messages, SourceQueues {}, {} messages, " +
                            "Overall consumption target: {}, Individual Source Queue consumption target: {}",
                    targetQueue.getName(), lastKnownTargetQueueLength,
                    (long) sourceQueues.size(), totalKnownPendingMessages,
                    consumptionTarget, sourceQueueConsumptionTarget);

            if(consumptionTarget <= 0) {
                LOGGER.info("Target queue '{}' consumption target is <= 0, no capacity for new messages, ignoring.", targetQueue.getName());
            }
            else {
                for(final Queue sourceQueue: sourceQueues) {
                    if(retrievedShovels.stream().anyMatch(s -> s.getName().endsWith(sourceQueue.getName()))) {
                        continue;
                    } else {
                        final Shovel shovel = new Shovel();
                        shovel.setSrcDeleteAfter(sourceQueueConsumptionTarget);
                        shovel.setAckMode("on-confirm");
                        shovel.setSrcQueue(sourceQueue.getName());
                        shovel.setSrcUri("amqp://david-cent01.swinfra.net:5672");
                        shovel.setDestQueue(targetQueue.getName());
                        shovel.setDestUri("amqp://david-cent01.swinfra.net:5672");
                        shovelsApi.getApi().putShovel("/", sourceQueue.getName(), 
                                new Component<>("shovel", sourceQueue.getName(), shovel));
                    }
                }
            }            
            
        }
        
    }

    private Set<Queue> getMesssageTargetsQueues(final List<Queue> queues) {

        return queues.stream()
                .filter(q ->
                        !q.getName().contains(MessageRouter.LOAD_BALANCED_INDICATOR) && q.getName().contains("classification")
                )
                .collect(Collectors.toSet());

    }

    private Set<Queue> getSourceQueues(final Queue targetQueue, final List<Queue> queues) {

        return queues.stream()
                .filter(q ->
                        q.getName().startsWith(targetQueue.getName() + MessageRouter.LOAD_BALANCED_INDICATOR)  && q.getName().contains("classification")
                )
                .collect(Collectors.toSet());
    }
    
    
    
}
