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

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.List;

import jakarta.ws.rs.ProcessingException;
import jakarta.ws.rs.client.Invocation;
import jakarta.ws.rs.client.WebTarget;
import jakarta.ws.rs.core.GenericType;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

public final class NodesApiImpl extends RabbitManagementApi  implements NodesApi {

    public NodesApiImpl(final String endpoint, final String user, final String password) {
        super(endpoint, user, password);
    }

    @Override
    public List<Node> getNodes(final String columnsCsvString) {
        final String url = endpoint + "/api/nodes/";

        try {
            final WebTarget target = client.target(url);
            if (columnsCsvString != null && !columnsCsvString.isBlank())
            {
                target.queryParam("columns", URLEncoder.encode(columnsCsvString, StandardCharsets.UTF_8.name()));
            }
            final Invocation.Builder builder = target.request(MediaType.APPLICATION_JSON)
                .header(HttpHeaders.AUTHORIZATION, authorizationHeaderValue);

            final Response response = builder.get();
            return response.readEntity(new GenericType<List<Node>>() {});
        } catch (final ProcessingException | UnsupportedEncodingException e) {
            throw new RuntimeException(prepareErrorMessage(url, null, e), e);
        }
    }

}
