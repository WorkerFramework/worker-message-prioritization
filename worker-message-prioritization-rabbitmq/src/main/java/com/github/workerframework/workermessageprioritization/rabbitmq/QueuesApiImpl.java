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
package com.github.workerframework.workermessageprioritization.rabbitmq;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.List;

import javax.ws.rs.ProcessingException;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.google.common.net.UrlEscapers;

public final class QueuesApiImpl extends RabbitManagementApi implements QueuesApi {

    public QueuesApiImpl(final String endpoint, final String user, final String password) {
        super(endpoint, user, password);
    }

    @Override
    public List<Queue> getQueues() throws QueueNotFoundException {
        return getRabbitQueues(null);
    }

    @Override
    public List<Queue> getQueues(final String columnsCsvString) {
        return getRabbitQueues(columnsCsvString);
    }

    @Override
    public Queue getQueue(final String vhost, final String queueName) {
        final String url
            = endpoint
            + "/api/queues/"
            + UrlEscapers.urlPathSegmentEscaper().escape(vhost)
            + "/"
            + UrlEscapers.urlPathSegmentEscaper().escape(queueName);
        try {
            final Invocation.Builder builder
                = client
                    .target(url)
                    .request(MediaType.APPLICATION_JSON)
                    .header(HttpHeaders.AUTHORIZATION, authorizationHeaderValue);

            final Response response = builder.get();

            final int status = response.getStatus();
            if (status == 200) {
                return response.readEntity(Queue.class);
            } else if (status == 404) {
                throw new QueueNotFoundException(url);
            } else {
                throw new RuntimeException(prepareErrorMessage(url, response, null));
            }
        } catch (final ProcessingException e) {
            throw new RuntimeException(prepareErrorMessage(url, null, e), e);
        }
    }

    private List<Queue> getRabbitQueues(final String columnsCsvString) {
        final String url = endpoint + "/api/queues/";

        try {
            final WebTarget webTarget = client.target(url);

            if(columnsCsvString != null) {
                webTarget.queryParam("columns", URLEncoder.encode(columnsCsvString, StandardCharsets.UTF_8.name()));
            }

            final Invocation.Builder builder = webTarget
                    .request(MediaType.APPLICATION_JSON)
                    .header(HttpHeaders.AUTHORIZATION, authorizationHeaderValue);

            final Response response = builder.get();

            final int status = response.getStatus();
            if (status == 200) {
                return response.readEntity(new GenericType<List<Queue>>() {});
            } else if (status == 404) {
                throw new QueueNotFoundException(url);
            } else {
                throw new RuntimeException(prepareErrorMessage(url, response, null));
            }
        } catch (final ProcessingException | UnsupportedEncodingException e) {
            throw new RuntimeException(prepareErrorMessage(url, null, e), e);
        }
    }

}
