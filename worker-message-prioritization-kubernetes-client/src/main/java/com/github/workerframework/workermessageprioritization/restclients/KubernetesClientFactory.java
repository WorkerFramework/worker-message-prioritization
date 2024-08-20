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
import com.github.workerframework.workermessageprioritization.restclients.kubernetes.api.AppsV1Api;
import com.github.workerframework.workermessageprioritization.restclients.kubernetes.client.ApiClient;
import com.github.workerframework.workermessageprioritization.restclients.kubernetes.model.IoK8sApiAppsV1DeploymentList;

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

    //curl --cacert /var/run/secrets/kubernetes.io/serviceaccount/ca.crt -H "Authorization: Bearer $(cat /var/run/secrets/kubernetes.io/serviceaccount/token)" https://192.168.128.1:443/api
    // KUBERNETES_SERVICE_PORT_HTTPS=443
    // KUBERNETES_SERVICE_PORT=443
    // KUBERNETES_SERVICE_HOST=192.168.128.1

    // Got these from:
    // https://16.103.45.94:6443/api
    // https://github.houston.softwaregrp.net/Verity/deploy/blob/master/override/kube-config-larry
    /// curl --cacert /var/run/secrets/kubernetes.io/serviceaccount/ca.crt -H "Authorization: Bearer $(cat /var/run/secrets/kubernetes.io/serviceaccount/token)" https://16.103.45.94:6443/apis/apps/v1/namespaces/private/deployments
    public static void main(String[] args) throws Exception
    {
        final ApiClient apiClient = KubernetesClientFactory.createClientWithCertAndToken(
                "C:\\Users\\RTorney\\Documents\\ca.crt",
                "C:\\Users\\RTorney\\Documents\\token",
                "16.103.45.94",
                6443
        );
        AppsV1Api appsV1Api = new AppsV1Api(apiClient);


        final IoK8sApiAppsV1DeploymentList deploymentList = appsV1Api.listAppsV1NamespacedDeployment(
                "private",
                null,
                false,
                null,
                null,
                null,
                Integer.MAX_VALUE,
                null,
                null,
                null,
                0,
                null);

        System.out.println(deploymentList.getItems());
    }

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
        // Load the CA certificate
        final CertificateFactory certificateFactory = CertificateFactory.getInstance("X.509");
        final Certificate caCert;
        try (final FileInputStream caCertFileInputStream = new FileInputStream(caCertPath)) {
            caCert = certificateFactory.generateCertificate(caCertFileInputStream);
        } catch (final IOException e) {
            throw new RuntimeException("Cannot read ca cert file: " + caCertPath, e);
        }

        // Create a KeyStore with the CA certificate
        final KeyStore keyStore = newEmptyKeyStore();
        keyStore.setCertificateEntry("ca-cert", caCert);

        // Initialize the TrustManager with the CA certificate
        TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        trustManagerFactory.init(keyStore);

        // Set up the SSLContext with the validated CA certificate
        final SSLContext sslContext = SSLContext.getInstance("TLS");
        sslContext.init(null, trustManagerFactory.getTrustManagers(), new SecureRandom());

        // Configure ObjectMapper with JavaTimeModule
        final ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());

        // Create a JacksonJsonProvider with the configured ObjectMapper
        final JacksonJsonProvider jacksonJsonProvider = new JacksonJsonProvider(objectMapper);

        // Create ApiClient (this wraps the HTTP client)
        final ApiClient apiClient = new ApiClient();
        apiClient.setBasePath(new URI("https", null, host, port, null, null, null).toString());
        apiClient.setReadTimeout(0);

        // Get the ClientConfig and register the JacksonJsonProvider
        final ClientConfig clientConfig = apiClient.getClientConfig();
        clientConfig.register(jacksonJsonProvider);

        // Build the HTTP client
        final Client client = ClientBuilder.newBuilder()
                .withConfig(clientConfig)
                .sslContext(sslContext)
                .hostnameVerifier(new Rfc2818HostnameVerifier())
                .register(new TokenFileAuthentication(tokenPath))
                .build();

        // Set the HTTP client on the ApiClient
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
