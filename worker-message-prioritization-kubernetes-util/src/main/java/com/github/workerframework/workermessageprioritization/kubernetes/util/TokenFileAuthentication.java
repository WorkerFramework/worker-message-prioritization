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
package com.github.workerframework.workermessageprioritization.kubernetes.util;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;

import jakarta.ws.rs.client.ClientRequestContext;
import jakarta.ws.rs.client.ClientRequestFilter;

/**
 * A Jersey implementation of <a href="https://github.com/kubernetes-client/java/blob/automated-release-21.0.1/util/src/main/java/io/kubernetes/client/util/credentials/TokenFileAuthentication.java">io.kubernetes.client.util.credentials.TokenFileAuthentication</a>.
 */
final class TokenFileAuthentication implements ClientRequestFilter
{
    private final String file;
    private String token;
    private Instant expiry;

    public TokenFileAuthentication(final String file)
    {
        this.expiry = Instant.MIN;
        this.file = file;
    }

    private String getToken()
    {
        if (Instant.now().isAfter(this.expiry)) {
            try {
                this.token = new String(Files.readAllBytes(Paths.get(this.file)), Charset.defaultCharset()).trim();
                expiry = Instant.now().plusSeconds(60);
            } catch (final IOException ie) {
                throw new RuntimeException("Cannot read file: " + this.file, ie);
            }
        }
        return this.token;
    }

    @Override
    public void filter(final ClientRequestContext requestContext)
    {
        requestContext.getHeaders().add("Authorization", "Bearer " + getToken());
    }
}
