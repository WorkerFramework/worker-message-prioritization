/*
 * Copyright 2022-2025 Open Text.
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
package com.github.workerframework.workermessageprioritization.rabbitmq;

import jakarta.ws.rs.ProcessingException;
import jakarta.ws.rs.client.Invocation;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import com.fasterxml.jackson.databind.JsonNode;

public final class HealthCheckApiImpl extends RabbitManagementApi implements HealthCheckApi {

    public HealthCheckApiImpl(final String endpoint, final String user, final String password) {
        super(endpoint, user, password);
    }

    @Override
    public JsonNode checkHealth() {
        // Responds with a 200 OK if all virtual hosts and running on the target node, otherwise responds with a 503 Service Unavailable.
        final String url = endpoint + "/api/health/checks/virtual-hosts";
        try {
            final Invocation.Builder builder = client.target(url)
                    .request(MediaType.APPLICATION_JSON)
                    .header(HttpHeaders.AUTHORIZATION, authorizationHeaderValue);

            final Response response = builder.get();
            return response.readEntity(JsonNode.class);
        } catch (final ProcessingException e) {
            throw new RuntimeException(prepareErrorMessage(url, null, e), e);
        }
    }

}
