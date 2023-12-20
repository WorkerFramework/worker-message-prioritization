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
package com.github.workerframework.workermessageprioritization.rabbitmq;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.List;

import javax.ws.rs.ProcessingException;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

public final class NodesApiImpl extends RabbitManagementApi  implements NodesApi {

    public NodesApiImpl(final String endpoint, final String user, final String password) {
        super(endpoint, user, password);
    }

    @Override
    public List<Node> getNodes(final String columnsCsvString) {
        final String url = endpoint + "/api/nodes/";

        try {
            final Invocation.Builder builder = client.target(url)
                    .queryParam("columns", URLEncoder.encode(columnsCsvString, StandardCharsets.UTF_8.name()))
                    .request(MediaType.APPLICATION_JSON)
                    .header(HttpHeaders.AUTHORIZATION, authorizationHeaderValue);

            final Response response = builder.get();
            return response.readEntity(new GenericType<List<Node>>() {});
        } catch (final ProcessingException | UnsupportedEncodingException e) {
            throw new RuntimeException(prepareErrorMessage(url, null, e), e);
        }
    }

}
