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
package com.github.workerframework.workermessageprioritization.redistribution;

import com.github.workerframework.workermessageprioritization.rabbitmq.QueuesApi;
import com.github.workerframework.workermessageprioritization.rabbitmq.RabbitManagementApi;
import com.github.workerframework.workermessageprioritization.redistribution.config.MessageDistributorConfig;
import com.github.workerframework.workermessageprioritization.redistribution.consumption.ConsumptionTargetCalculator;
import com.github.workerframework.workermessageprioritization.redistribution.consumption.EqualConsumptionTargetCalculator;
import com.github.workerframework.workermessageprioritization.redistribution.lowlevel.StagingTargetPairProvider;
import com.github.workerframework.workermessageprioritization.targetqueue.K8sTargetQueueSettingsProvider;
import com.github.workerframework.workermessageprioritization.targetqueue.TunedTargetQueueLengthProvider;
import com.github.workerframework.workermessageprioritization.targetqueue.TargetQueueSettingsProvider;
import com.github.workerframework.workermessageprioritization.targetqueue.HistoricalConsumptionRateManager;
import com.github.workerframework.workermessageprioritization.targetqueue.TargetQueueLengthRounder;
import com.github.workerframework.workermessageprioritization.targetqueue.QueueConsumptionRateProvider;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.TypeLiteral;
import com.google.inject.name.Named;
import com.rabbitmq.client.ConnectionFactory;

import java.util.List;
import java.util.Map;

public class DistributorModule extends AbstractModule {

    @Provides
    @Named("KubernetesNamespaces")
    List<String> provideKubernetesNamespaces(final MessageDistributorConfig messageDistributorConfig) {
        return messageDistributorConfig.getKubernetesNamespaces();
    }
    
    @Provides
    @Named("KubernetesLabelCacheExpiryMinutes")
    int provideKubernetesLabelCacheExpiryMinutes(final MessageDistributorConfig messageDistributorConfig) {
        return messageDistributorConfig.getKubernetesLabelCacheExpiryMinutes();
    }

    @Provides
    @Named("MaxConsumptionRateHistorySize")
    int provideMaxConsumptionRateHistorySize(final MessageDistributorConfig messageDistributorConfig){
        return messageDistributorConfig.getMaxConsumptionRateHistorySize();
    }

    @Provides
    @Named("MinConsumptionRateHistorySize")
    int provideMinConsumptionRateHistorySize(final MessageDistributorConfig messageDistributorConfig){
        return messageDistributorConfig.getMinConsumptionRateHistorySize();
    }

    @Provides
    @Named("RoundingMultiple")
    int provideRoundingMultiple(final MessageDistributorConfig messageDistributorConfig){
        return messageDistributorConfig.getRoundingMultiple();
    }

    @Provides
    @Named("RabbitMQMgmtUrl")
    String provideRabbitMQMgmtUrl(final MessageDistributorConfig messageDistributorConfig){
        return messageDistributorConfig.getRabbitMQMgmtUrl();
    }

    @Provides
    @Named("RabbitMQUsername")
    String provideRabbitMQUsername(final MessageDistributorConfig messageDistributorConfig){
        return messageDistributorConfig.getRabbitMQMgmtUsername();
    }

    @Provides
    @Named("RabbitMQPassword")
    String provideRabbitMQPassword(final MessageDistributorConfig messageDistributorConfig){
        return messageDistributorConfig.getRabbitMQPassword();
    }

    @Provides
    @Named("ApiType")
    Class<QueuesApi> provideApiType(){
        return QueuesApi.class;
    }

    @Provides
    @Named("NoOpMode")
    boolean provideNoOpMode(final MessageDistributorConfig messageDistributorConfig){
        return messageDistributorConfig.getNoOpMode();
    }

    @Provides
    @Named("QueueProcessingTimeGoalSeconds")
    double provideQueueProcessingTimeGoalSeconds(final MessageDistributorConfig messageDistributorConfig){
        return messageDistributorConfig.getQueueProcessingTimeGoalSeconds();
    }

    @Provides
    @Named("DistributorRunIntervalMilliseconds")
    long provideDistributorRunIntervalMilliseconds(final MessageDistributorConfig messageDistributorConfig){
        return messageDistributorConfig.getDistributorRunIntervalMilliseconds();
    }

    @Provides
    @Named("ConsumerPublisherPairLastDoneWorkTimeoutMilliseconds")
    long provideConsumerPublisherPairLastDoneWorkTimeoutMilliseconds(final MessageDistributorConfig messageDistributorConfig){
        return messageDistributorConfig.getConsumerPublisherPairLastDoneWorkTimeoutMilliseconds();
    }

    @Provides
    @Named("MaxTargetQueueLength")
    long provideMaxTargetQueueLength(final MessageDistributorConfig messageDistributorConfig){
        return messageDistributorConfig.getMaxTargetQueueLength();
    }

    @Provides
    @Named("MinTargetQueueLength")
    long provideMinTargetQueueLength(final MessageDistributorConfig messageDistributorConfig){
        return messageDistributorConfig.getMinTargetQueueLength();
    }

    @Provides
    ConnectionFactory provideConnectionFactory(final MessageDistributorConfig messageDistributorConfig) {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost(messageDistributorConfig.getRabbitMQHost());
        connectionFactory.setUsername(messageDistributorConfig.getRabbitMQUsername());
        connectionFactory.setPassword(messageDistributorConfig.getRabbitMQPassword());
        connectionFactory.setPort(messageDistributorConfig.getRabbitMQPort());
        connectionFactory.setVirtualHost(messageDistributorConfig.getRabbitMQVHost());
        return connectionFactory;
    }

    @Override
    protected void configure() {
        bind(MessageDistributorConfig.class).in(Scopes.SINGLETON);
        bind(TargetQueueSettingsProvider.class).to(K8sTargetQueueSettingsProvider.class);
        bind(HistoricalConsumptionRateManager.class);
        bind(TargetQueueLengthRounder.class);
        bind(QueueConsumptionRateProvider.class);
        bind(new TypeLiteral<RabbitManagementApi<QueuesApi>>(){});
        bind(StagingTargetPairProvider.class);
        bind(TunedTargetQueueLengthProvider.class);
        bind(ConsumptionTargetCalculator.class).to(EqualConsumptionTargetCalculator.class);
        
        
    }
}
