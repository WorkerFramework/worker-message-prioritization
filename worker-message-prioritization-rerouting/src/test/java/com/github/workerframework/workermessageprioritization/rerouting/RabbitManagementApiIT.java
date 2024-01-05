/*
 * Copyright 2022-2024 Open Text.
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
package com.github.workerframework.workermessageprioritization.rerouting;

import com.fasterxml.jackson.databind.JsonNode;
import com.github.workerframework.workermessageprioritization.rabbitmq.Node;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public final class RabbitManagementApiIT extends RerouterTestBase {

    @Test
    public void healthCheckTest() {
        final JsonNode health = healthCheckApi.checkHealth();
        Assert.assertEquals("Health check not ok", "ok", health.get("status").asText());
    }

    @Test
    public void getNodesTest() {
        final List<Node> nodes = nodesApi.getNodes(null);
        Assert.assertEquals("One node not found", 1, nodes.size());
    }

}
