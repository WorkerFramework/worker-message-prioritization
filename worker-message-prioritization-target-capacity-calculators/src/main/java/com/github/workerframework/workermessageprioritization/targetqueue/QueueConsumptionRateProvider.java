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

import com.github.workerframework.workermessageprioritization.rabbitmq.Queue;
import com.github.workerframework.workermessageprioritization.rabbitmq.QueuesApi;
import com.github.workerframework.workermessageprioritization.rabbitmq.RabbitManagementApi;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueueConsumptionRateProvider {

    protected RabbitManagementApi<QueuesApi> queuesApi;
    private static final Logger TUNED_TARGET_LOGGER = LoggerFactory.getLogger("TUNED_TARGET");

    @Inject
    public QueueConsumptionRateProvider(final RabbitManagementApi<QueuesApi> queuesApi){
        this.queuesApi = queuesApi;
    }

    public double getConsumptionRate(final String targetQueueName) {

        final Queue.MessageStats message_stats = queuesApi.getApi().getQueue("/", targetQueueName).getMessage_stats();
        final double consumer_capacity = queuesApi.getApi().getQueue("/", targetQueueName).getConsumer_Capacity();
        final double consumers = queuesApi.getApi().getQueue("/", targetQueueName).getConsumers();
        final double consumptionRate;

        TUNED_TARGET_LOGGER.info("Current consumer_capacity of " + targetQueueName + " is: " + consumer_capacity);
        TUNED_TARGET_LOGGER.info("Current consumers of " + targetQueueName + " is: " + consumers);

        if (message_stats != null) {
            if(message_stats.getDeliver_get_details() != null){
                consumptionRate = message_stats.getDeliver_get_details().getRate();
            }else{
                consumptionRate = 0D;
            }
        } else {
            consumptionRate = 0D;
        }

        return consumptionRate;
    }

}
