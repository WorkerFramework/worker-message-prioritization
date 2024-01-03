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

import java.nio.charset.StandardCharsets;
import java.util.Base64;

import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.client.ClientBuilder;
import jakarta.ws.rs.core.Response;

import org.glassfish.jersey.client.ClientProperties;

public abstract class RabbitManagementApi {

    private static final int READ_TIMEOUT_MILLISECONDS = 20000;
    private static final int CONNECT_TIMEOUT_MILLISECONDS = 20000;

    protected final Client client;
    protected final String endpoint;
    protected final String authorizationHeaderValue;

    public RabbitManagementApi(final String endpoint, final String user, final String password) {
        this.client = ClientBuilder.newClient();
        this.client.register(JacksonConfigurator.class);
        client.property(ClientProperties.CONNECT_TIMEOUT, CONNECT_TIMEOUT_MILLISECONDS);
        client.property(ClientProperties.READ_TIMEOUT, READ_TIMEOUT_MILLISECONDS);

        this.endpoint = endpoint.endsWith("/") ? endpoint.substring(0, endpoint.length() -1) : endpoint;

        final String credentials = user + ":" + password;
        this.authorizationHeaderValue = "Basic " + Base64.getEncoder().encodeToString(credentials.getBytes(StandardCharsets.UTF_8));

    }

    protected static String prepareErrorMessage(final String url, final Response response, final Exception error)
    {
        final StringBuffer errorMessage = new StringBuffer("RabbitMQ management API error: ");
        errorMessage.append("requestUrl=").append(url);

        if(response != null) {
            errorMessage
                .append("responseStatus=").append(String.valueOf(response.getStatus()))
                .append("responseBody=").append(response.readEntity(String.class));
        }

        if(error != null) {
            errorMessage.append("cause=").append(error.getCause());
        }

        return errorMessage.toString();
    }
}
