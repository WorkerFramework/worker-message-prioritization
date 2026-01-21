/*
 * Copyright 2022-2026 Open Text.
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

import java.util.List;

import com.github.workerframework.workermessageprioritization.targetqueue.FallbackTargetQueueSettingsProvider;
import com.github.workerframework.workermessageprioritization.targetqueue.K8sTargetQueueSettingsProvider;
import com.github.workerframework.workermessageprioritization.targetqueue.TargetQueueSettingsProvider;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.name.Named;

final class TargetQueueSettingsProviderProvider implements Provider<TargetQueueSettingsProvider>
{
    private final boolean kubernetesEnabled;
    private final List<String> kubernetesNamespaces;
    private final int kubernetesLabelCacheExpiryMinutes;

    @Inject
    public TargetQueueSettingsProviderProvider(
            @Named("KubernetesEnabled") final boolean kubernetesEnabled,
            @Named("KubernetesNamespaces") final List<String> kubernetesNamespaces,
            @Named("KubernetesLabelCacheExpiryMinutes") final int kubernetesLabelCacheExpiryMinutes)
    {
        this.kubernetesEnabled = kubernetesEnabled;
        this.kubernetesNamespaces = kubernetesNamespaces;
        this.kubernetesLabelCacheExpiryMinutes = kubernetesLabelCacheExpiryMinutes;
    }

    @Override
    public TargetQueueSettingsProvider get()
    {
        if (kubernetesEnabled) {
            return new K8sTargetQueueSettingsProvider(kubernetesNamespaces, kubernetesLabelCacheExpiryMinutes);
        } else {
            return new FallbackTargetQueueSettingsProvider();
        }
    }
}
