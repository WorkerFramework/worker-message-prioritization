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
package com.github.workerframework.workermessageprioritization.restclients;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.jackson.internal.jackson.jaxrs.json.JacksonJsonProvider;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.github.workerframework.workermessageprioritization.restclients.kubernetes.client.ApiClient;

import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.client.ClientBuilder;

/**
 * This class is loosely based on
 * <a href="https://github.com/kubernetes-client/java/blob/automated-release-21.0.1/util/src/main/java/io/kubernetes/client/util/ClientBuilder.java#L70">io.kubernetes.client.util.ClientBuilder.java</a>,
 * which we stopped using because it brought in a lot of unwanted dependencies (such as the AWS SDK, Google Protocol Buffers, and the
 * Bouncy Castle libraries).
 */
public final class KubernetesClientFactory
{
    private static final String SERVICEACCOUNT_ROOT = "/var/run/secrets/kubernetes.io/serviceaccount";
    private static final String SERVICEACCOUNT_CA_PATH = SERVICEACCOUNT_ROOT + "/ca.crt";
    private static final String SERVICEACCOUNT_TOKEN_PATH = SERVICEACCOUNT_ROOT + "/token";
    private static final String ENV_SERVICE_HOST = "KUBERNETES_SERVICE_HOST";
    private static final String ENV_SERVICE_PORT = "KUBERNETES_SERVICE_PORT";

    private KubernetesClientFactory()
    {
    }

    /**
     * Creates a Kubernetes client with a CA cert and Bearer token.
     * <p>
     * This method expects the following environment variables to be preset:
     * <ul>
     *   <li>KUBERNETES_SERVICE_HOST</li>
     *   <li>KUBERNETES_SERVICE_PORT</li>
     * </ul>
     * <p>
     * and the following files to be present:
     * <ul>
     *   <li>/var/run/secrets/kubernetes.io/serviceaccount/ca.crt</li>
     *   <li>/var/run/secrets/kubernetes.io/serviceaccount/token</li>
     * </ul>
     *
     * @return a client configured to communicate with Kubernetes using a CA cert and Bearer token.
     * @throws Exception if the client could not be created for any reason.
     */
    public static ApiClient createClientWithCertAndToken() throws Exception
    {
        final String host = System.getenv(ENV_SERVICE_HOST);
        if (host == null || host.isEmpty()) {
            throw new RuntimeException("Environment variable not set: " + ENV_SERVICE_HOST);
        }

        final String port = System.getenv(ENV_SERVICE_PORT);
        if (port == null || port.isEmpty()) {
            throw new RuntimeException("Environment variable not set: " + ENV_SERVICE_PORT);
        }

        return createClientWithCertAndToken(
                SERVICEACCOUNT_CA_PATH,
                SERVICEACCOUNT_TOKEN_PATH,
                host,
                Integer.parseInt(port)
        );
    }

    // Visible for testing
    public static ApiClient createClientWithCertAndToken(
            final String caCertPath,
            final String tokenPath,
            final String host,
            final int port
    ) throws Exception
    {
        //
        // SSLContext
        //

        final CertificateFactory certificateFactory = CertificateFactory.getInstance("X.509");
        final Certificate caCert;
        try (final FileInputStream caCertFileInputStream = new FileInputStream(caCertPath)) {
            caCert = certificateFactory.generateCertificate(caCertFileInputStream);
        } catch (final IOException e) {
            throw new RuntimeException("Cannot read ca cert file: " + caCertPath, e);
        }

        final KeyStore keyStore = newEmptyKeyStore();
        keyStore.setCertificateEntry("ca-cert", caCert);

        TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        trustManagerFactory.init(keyStore);

        final SSLContext sslContext = SSLContext.getInstance("TLS");
        sslContext.init(null, trustManagerFactory.getTrustManagers(), new SecureRandom());

        //
        // ObjectMapper
        //

        final ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());
        final JacksonJsonProvider jacksonJsonProvider = new JacksonJsonProvider(objectMapper);

        //
        // ApiClient
        //

        final ApiClient apiClient = new ApiClient();
        apiClient.setBasePath(new URI("https", null, host, port, null, null, null).toString());
        apiClient.setReadTimeout(0);

        final ClientConfig clientConfig = apiClient.getClientConfig();
        clientConfig.register(jacksonJsonProvider);

        final Client client = ClientBuilder.newBuilder()
                .withConfig(clientConfig)
                .sslContext(sslContext)
                .hostnameVerifier(new Rfc2818HostnameVerifier())
                .register(new TokenFileAuthentication(tokenPath))
                .build();

        apiClient.setHttpClient(client);

        return apiClient;
    }

    private static KeyStore newEmptyKeyStore() throws Exception
    {
        final KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
        keyStore.load(null, null);
        return keyStore;
    }
}
