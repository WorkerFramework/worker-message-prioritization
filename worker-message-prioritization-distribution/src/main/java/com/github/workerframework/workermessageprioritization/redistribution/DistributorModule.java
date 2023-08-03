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

import com.github.workerframework.workermessageprioritization.redistribution.config.MessageDistributorConfig;
import com.github.workerframework.workermessageprioritization.redistribution.consumption.ConsumptionTargetCalculator;
import com.github.workerframework.workermessageprioritization.redistribution.consumption.EqualConsumptionTargetCalculator;
import com.github.workerframework.workermessageprioritization.targetqueue.K8sTargetQueueSettingsProvider;
import com.github.workerframework.workermessageprioritization.targetqueue.TargetQueueSettingsProvider;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.name.Named;

import java.util.List;

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

    @Override
    protected void configure() {
        bind(MessageDistributorConfig.class).in(Scopes.SINGLETON);
//        bind(TargetQueueSettingsProvider.class).to(K8sTargetQueueSettingsProvider.class);
        
        //If there is only one implementation we don't need to bind, just make sure the constructor is marked with @Inject
//        bind(TunedTargetQueueLengthProvider.class);
//        bind(ConsumptionTargetCalculator.class).to(EqualConsumptionTargetCalculator.class);
        
        
    }
}
