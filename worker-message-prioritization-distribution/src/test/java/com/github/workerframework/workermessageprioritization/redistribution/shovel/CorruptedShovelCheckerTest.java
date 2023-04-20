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
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.github.workerframework.workermessageprioritization.rabbitmq.Node;
import com.github.workerframework.workermessageprioritization.rabbitmq.NodesApi;
import com.github.workerframework.workermessageprioritization.rabbitmq.RabbitManagementApi;
import com.github.workerframework.workermessageprioritization.rabbitmq.RetrievedShovel;
import com.github.workerframework.workermessageprioritization.rabbitmq.ShovelFromParametersApi;
import com.github.workerframework.workermessageprioritization.rabbitmq.ShovelsApi;
import com.google.common.cache.LoadingCache;

import retrofit.RetrofitError;
import retrofit.client.Response;

public class CorruptedShovelCheckerTest
{
    @Mock
    private RabbitManagementApi<ShovelsApi> mockRabbitManagementApiShovelsApi;

    @Mock
    private RabbitManagementApi<ShovelsApi> mockNode1RabbitManagementApiShovelsApi;

    @Mock
    private RabbitManagementApi<ShovelsApi> mockNode2RabbitManagementApiShovelsApi;

    @Mock
    private RabbitManagementApi<NodesApi> mockRabbitManagementApiNodesApi;

    @Mock
    private ShovelsApi mockShovelsApi;

    @Mock
    private ShovelsApi mockNode1ShovelsApi;

    @Mock
    private ShovelsApi mockNode2ShovelsApi;

    @Mock
    private NodesApi mockNodesApi;

    @Mock
    LoadingCache<String,RabbitManagementApi<ShovelsApi>> mockNodeSpecificShovelsApiCache;

    private CorruptedShovelChecker corruptedShovelChecker;

    private static final String VHOST = "/";

    @Before
    public void setUp()
    {
        MockitoAnnotations.initMocks(this);
        when(mockRabbitManagementApiShovelsApi.getApi()).thenReturn(mockShovelsApi);
        corruptedShovelChecker = new CorruptedShovelChecker(
                mockRabbitManagementApiShovelsApi, mockRabbitManagementApiNodesApi, mockNodeSpecificShovelsApiCache, VHOST, 0, 0);
    }

    //@Test
    public void testNoCorruptedShovels() throws ExecutionException
    {
        // 2 good shovels
        when(mockShovelsApi.getShovelsFromParametersApi(VHOST)).thenReturn(makeShovelsFromParametersApi(
                "goodShovel1", "goodShovel2"));
        when(mockShovelsApi.getShovels(VHOST)).thenReturn(makeShovelsFromNonParametersApi(
                "goodShovel1", "goodShovel2"));

        // 2 nodes
        when(mockRabbitManagementApiNodesApi.getApi()).thenReturn(mockNodesApi);
        when(mockNodesApi.getNodes("name")).thenReturn(makeNodes("node1", "node2"));
        when(mockNodeSpecificShovelsApiCache.get("node1")).thenReturn(mockNode1RabbitManagementApiShovelsApi);
        when(mockNode1RabbitManagementApiShovelsApi.getApi()).thenReturn(mockNode1ShovelsApi);
        when(mockNodeSpecificShovelsApiCache.get("node2")).thenReturn(mockNode2RabbitManagementApiShovelsApi);
        when(mockNode2RabbitManagementApiShovelsApi.getApi()).thenReturn(mockNode2ShovelsApi);

        // Run the test
        corruptedShovelChecker.run();

        // Verity that the good shovels were not deleted
        verify(mockNode1ShovelsApi, never()).delete(VHOST, "goodShovel1");
        verify(mockNode2ShovelsApi, never()).delete(VHOST, "goodShovel1");
        verify(mockNode1ShovelsApi, never()).delete(VHOST, "goodShovel2");
        verify(mockNode2ShovelsApi, never()).delete(VHOST, "goodShovel2");
    }

    @Test
    public void testCorruptedShovelsFound() throws ExecutionException
    {
        // 1 good shovel, 2 corrupted shovels
        when(mockShovelsApi.getShovelsFromParametersApi(VHOST)).thenReturn(makeShovelsFromParametersApi(
                "goodShovel1", "corruptedShovel1", "corruptedShovel2"));
        when(mockShovelsApi.getShovels(VHOST)).thenReturn(makeShovelsFromNonParametersApi(
                "goodShovel1"));

        // 2 nodes
        when(mockRabbitManagementApiNodesApi.getApi()).thenReturn(mockNodesApi);
        when(mockNodesApi.getNodes("name")).thenReturn(makeNodes("node1", "node2"));
        when(mockNodeSpecificShovelsApiCache.get("node1")).thenReturn(mockNode1RabbitManagementApiShovelsApi);
        when(mockNode1RabbitManagementApiShovelsApi.getApi()).thenReturn(mockNode1ShovelsApi);
        when(mockNodeSpecificShovelsApiCache.get("node2")).thenReturn(mockNode2RabbitManagementApiShovelsApi);
        when(mockNode2RabbitManagementApiShovelsApi.getApi()).thenReturn(mockNode2ShovelsApi);

        // After deletion, these calls are made to check if the corrupted shovels have been deleted
        when(mockShovelsApi.getShovelFromParametersApi(VHOST, "corruptedShovel1"))
                .thenThrow(makeRetrofitError(404));
        when(mockShovelsApi.getShovelFromParametersApi(VHOST, "corruptedShovel2"))
                .thenThrow(makeRetrofitError(404));

        // Run the test
        corruptedShovelChecker.run();

        // Verify that the 2 corrupted shovels were deleted
        verify(mockNode1ShovelsApi).delete(VHOST, "corruptedShovel1");
        verify(mockNode2ShovelsApi).delete(VHOST, "corruptedShovel1");
        verify(mockNode1ShovelsApi).delete(VHOST, "corruptedShovel2");
        verify(mockNode2ShovelsApi).delete(VHOST, "corruptedShovel2");

        // Verity that the good shovel was not deleted
        verify(mockNode1ShovelsApi, never()).delete(VHOST, "goodShovel1");
        verify(mockNode2ShovelsApi, never()).delete(VHOST, "goodShovel1");
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

    private static List<Node> makeNodes(final String ... nodeNames)
    {
        final List<Node> nodes = new ArrayList<>(nodeNames.length);

        for (final String nodeName : nodeNames) {
            final Node node = new Node();
            node.setName(nodeName);
            nodes.add(node);
        }

        return nodes;
    }

    private static RuntimeException makeRetrofitError(final int status)
    {
        final Response response = new Response("", status, "", Collections.emptyList(), null);

        final RetrofitError retrofitError = RetrofitError.httpError("", response, null, null);

        return new RuntimeException(retrofitError);
    }
}
