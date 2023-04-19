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
package com.github.workerframework.workermessageprioritization.redistribution.shovel;

import static org.mockito.Mockito.*;

import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.github.workerframework.workermessageprioritization.rabbitmq.RabbitManagementApi;
import com.github.workerframework.workermessageprioritization.rabbitmq.RetrievedShovel;
import com.github.workerframework.workermessageprioritization.rabbitmq.ShovelFromParametersApi;
import com.github.workerframework.workermessageprioritization.rabbitmq.ShovelsApi;

public class CorruptedShovelCheckerTest
{
    @Mock
    private RabbitManagementApi<ShovelsApi> mockRabbitManagementApi;

    @Mock
    private ShovelsApi mockShovelsApi;

    private CorruptedShovelChecker corruptedShovelChecker;

    private static final String VHOST = "/";

    @Before
    public void setUp()
    {
        MockitoAnnotations.initMocks(this);
        when(mockRabbitManagementApi.getApi()).thenReturn(mockShovelsApi);
        corruptedShovelChecker = new CorruptedShovelChecker(mockRabbitManagementApi, nodeSpecificShovelsApiCache, VHOST, 0, 0);
    }

    @Test
    public void testNoCorruptedShovels()
    {
        when(mockShovelsApi.getShovelsFromParametersApi(VHOST)).thenReturn(makeShovelsFromParametersApi(
                "shovel1", "shovel2"));
        when(mockShovelsApi.getShovels(VHOST)).thenReturn(makeShovelsFromNonParametersApi(
                "shovel1", "shovel2"));

        corruptedShovelChecker.run();

        verify(mockShovelsApi, never()).delete(anyString(), anyString());
    }

    @Test
    public void testCorruptedShovelFound()
    {
        when(mockShovelsApi.getShovelsFromParametersApi(VHOST)).thenReturn(makeShovelsFromParametersApi(
                "shovel1", "shovel2"));
        when(mockShovelsApi.getShovels(VHOST)).thenReturn(makeShovelsFromNonParametersApi(
                "shovel1"));

        corruptedShovelChecker.run();

        verify(mockShovelsApi).delete(VHOST, "shovel2");
    }

    private static List<ShovelFromParametersApi> makeShovelsFromParametersApi(final String... names)
    {
        final List<ShovelFromParametersApi> shovelsFromParametersApi = new ArrayList<>(names.length);

        for (final String name : names) {
            final ShovelFromParametersApi shovelFromParametersApi = new ShovelFromParametersApi();
            shovelFromParametersApi.setName(name);
            shovelsFromParametersApi.add(shovelFromParametersApi);
        }

        return shovelsFromParametersApi;
    }

    private static List<RetrievedShovel> makeShovelsFromNonParametersApi(final String... names)
    {
        final List<RetrievedShovel> shovelsFromNonParametersApi = new ArrayList<>(names.length);

        for (final String name : names) {
            final RetrievedShovel shovelFromNonParametersApi = new RetrievedShovel();
            shovelFromNonParametersApi.setName(name);
            shovelsFromNonParametersApi.add(shovelFromNonParametersApi);
        }

        return shovelsFromNonParametersApi;
    }
}
