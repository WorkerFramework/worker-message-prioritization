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

import static io.kubernetes.client.util.Config.ENV_KUBECONFIG;
import static io.kubernetes.client.util.Config.ENV_SERVICE_HOST;
import static io.kubernetes.client.util.Config.ENV_SERVICE_PORT;
import static io.kubernetes.client.util.Config.SERVICEACCOUNT_CA_PATH;
import static io.kubernetes.client.util.Config.SERVICEACCOUNT_TOKEN_PATH;
import static io.kubernetes.client.util.KubeConfig.ENV_HOME;
import static io.kubernetes.client.util.KubeConfig.KUBECONFIG;
import static io.kubernetes.client.util.KubeConfig.KUBEDIR;

import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.models.V1CertificateSigningRequest;
import io.kubernetes.client.persister.FilePersister;
import io.kubernetes.client.util.credentials.AccessTokenAuthentication;
import io.kubernetes.client.util.credentials.Authentication;
import io.kubernetes.client.util.credentials.ClientCertificateAuthentication;
import io.kubernetes.client.util.credentials.KubeconfigAuthentication;
import io.kubernetes.client.util.credentials.TokenFileAuthentication;
import io.kubernetes.client.util.exception.CSRNotApprovedException;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import okhttp3.Protocol;
import org.apache.commons.compress.utils.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A Builder which allows the construction of {@link ApiClient}s in a fluent fashion. */
public class ClientBuilder {
    private static final Logger log = LoggerFactory.getLogger(ClientBuilder.class);

    private static final String SERVICEACCOUNT_ROOT = "/var/run/secrets/kubernetes.io/serviceaccount";
    private static final String SERVICEACCOUNT_CA_PATH = SERVICEACCOUNT_ROOT + "/ca.crt";
    private static final String SERVICEACCOUNT_TOKEN_PATH = SERVICEACCOUNT_ROOT + "/token";
    private static final String SERVICEACCOUNT_NAMESPACE_PATH = SERVICEACCOUNT_ROOT + "/namespace";
    private static final String ENV_KUBECONFIG = "KUBECONFIG";
    private static final String ENV_SERVICE_HOST = "KUBERNETES_SERVICE_HOST";
    private static final String ENV_SERVICE_PORT = "KUBERNETES_SERVICE_PORT";

    private String basePath = "http://localhost:8080";
    private byte[] caCertBytes = null;
    private boolean verifyingSsl = true;
    private Authentication authentication;
    private String keyStorePassphrase;
    // defaulting client protocols to HTTP1.1 and HTTP 2
    private List<Protocol> protocols = Arrays.asList(Protocol.HTTP_2, Protocol.HTTP_1_1);
    // default to unlimited read timeout
    private Duration readTimeout = Duration.ZERO;
    // default health check is once a minute
    private Duration pingInterval = Duration.ofMinutes(1);

    /**
     * Creates a builder which is pre-configured in the following way
     *
     * <ul>
     *   <li>If $KUBECONFIG is defined, use that config file.
     *   <li>If $HOME/.kube/config can be found, use that.
     *   <li>If the in-cluster service account can be found, assume in cluster config.
     *   <li>Default to localhost:8080 as a last resort.
     * </ul>
     *
     * @return <tt>ClientBuilder</tt> pre-configured using the above precedence
     * @throws IOException if the configuration file or a file specified in a configuration file
     *     cannot be read.
     */
    public static ClientBuilder standard() throws IOException {
        return standard(true);
    }

    public static ClientBuilder standard(boolean persistConfig) throws IOException {
        final File kubeConfig = findConfigFromEnv();
        ClientBuilder clientBuilderEnv = getClientBuilder(persistConfig, kubeConfig);
        if (clientBuilderEnv != null) return clientBuilderEnv;
        final File config = findConfigInHomeDir();
        ClientBuilder clientBuilderHomeDir = getClientBuilder(persistConfig, config);
        if (clientBuilderHomeDir != null) return clientBuilderHomeDir;
        final File clusterCa = new File(SERVICEACCOUNT_CA_PATH);
        if (clusterCa.exists()) {
            return cluster();
        }
        return new ClientBuilder();
    }

    private static ClientBuilder getClientBuilder(boolean persistConfig, File kubeConfig)
            throws IOException {
        if (kubeConfig != null) {
            try (BufferedReader kubeConfigReader =
                         new BufferedReader(
                                 new InputStreamReader(
                                         new FileInputStream(kubeConfig), StandardCharsets.UTF_8.name()))) {
                KubeConfig kc = KubeConfig.loadKubeConfig(kubeConfigReader);
                if (persistConfig) {
                    kc.setPersistConfig(new FilePersister(kubeConfig));
                }
                kc.setFile(kubeConfig);
                return kubeconfig(kc);
            }
        }
        return null;
    }

    private static File findConfigFromEnv() {
        final KubeConfigEnvParser kubeConfigEnvParser = new KubeConfigEnvParser();

        final String kubeConfigPath =
                kubeConfigEnvParser.parseKubeConfigPath(System.getenv(ENV_KUBECONFIG));
        if (kubeConfigPath == null) {
            return null;
        }
        final File kubeConfig = new File(kubeConfigPath);
        if (kubeConfig.exists()) {
            return kubeConfig;
        } else {
            log.debug("Could not find file specified in $KUBECONFIG");
            return null;
        }
    }

    private static class KubeConfigEnvParser {
        private String parseKubeConfigPath(String kubeConfigEnv) {
            if (kubeConfigEnv == null) {
                return null;
            }

            final String[] filePaths = kubeConfigEnv.split(File.pathSeparator);
            final String kubeConfigPath = filePaths[0];
            if (filePaths.length > 1) {
                log.warn("Found multiple kubeconfigs files, $KUBECONFIG: {} using first: {}", kubeConfigEnv, kubeConfigPath);
            }

            return kubeConfigPath;
        }
    }

    private static File findHomeDir() {
        final String envHome = System.getenv(ENV_HOME);
        if (envHome != null && envHome.length() > 0) {
            final File config = new File(envHome);
            if (config.exists()) {
                return config;
            }
        }
        if (System.getProperty("os.name").toLowerCase().startsWith("windows")) {
            String homeDrive = System.getenv("HOMEDRIVE");
            String homePath = System.getenv("HOMEPATH");
            if (homeDrive != null
                    && homeDrive.length() > 0
                    && homePath != null
                    && homePath.length() > 0) {
                File homeDir = new File(new File(homeDrive), homePath);
                if (homeDir.exists()) {
                    return homeDir;
                }
            }
            String userProfile = System.getenv("USERPROFILE");
            if (userProfile != null && userProfile.length() > 0) {
                File profileDir = new File(userProfile);
                if (profileDir.exists()) {
                    return profileDir;
                }
            }
        }
        return null;
    }

    private static File findConfigInHomeDir() {
        final File homeDir = findHomeDir();
        if (homeDir != null) {
            final File config = new File(new File(homeDir, KUBEDIR), KUBECONFIG);
            if (config.exists()) {
                return config;
            }
        }
        log.debug("Could not find ~/.kube/config");
        return null;
    }

    /**
     * Creates a builder which is pre-configured from the cluster configuration.
     *
     * @return <tt>ClientBuilder</tt> configured from the cluster configuration where service account
     *     token will be reloaded.
     * @throws IOException if the Service Account Token Path or CA Path is not readable.
     */
    public static ClientBuilder cluster() throws IOException {
        final ClientBuilder builder = new ClientBuilder();

        final String host = System.getenv(ENV_SERVICE_HOST);
        final String port = System.getenv(ENV_SERVICE_PORT);
        builder.setBasePath(host, port);

        builder.setCertificateAuthority(Files.readAllBytes(Paths.get(SERVICEACCOUNT_CA_PATH)));
        builder.setAuthentication(new TokenFileAuthentication(SERVICEACCOUNT_TOKEN_PATH));

        return builder;
    }

    protected ClientBuilder setBasePath(String host, String port) {
        try {
            Integer iPort = Integer.valueOf(port);
            URI uri = new URI("https", null, host, iPort, null, null, null);
            this.setBasePath(uri.toString());
        } catch (NumberFormatException | URISyntaxException e) {
            throw new IllegalStateException(e);
        }
        return this;
    }

    public String getBasePath() {
        return basePath;
    }

    public ClientBuilder setBasePath(String basePath) {
        this.basePath = basePath;
        return this;
    }

    public Authentication getAuthentication() {
        return authentication;
    }

    public ClientBuilder setAuthentication(final Authentication authentication) {
        this.authentication = authentication;
        return this;
    }

    public ClientBuilder setCertificateAuthority(final byte[] caCertBytes) {
        this.caCertBytes = caCertBytes;
        return this;
    }

    public boolean isVerifyingSsl() {
        return verifyingSsl;
    }

    public ClientBuilder setVerifyingSsl(boolean verifyingSsl) {
        this.verifyingSsl = verifyingSsl;
        return this;
    }

    public ClientBuilder setProtocols(List<Protocol> protocols) {
        this.protocols = protocols;
        return this;
    }

    public List<Protocol> getProtocols() {
        return protocols;
    }

    public ClientBuilder setReadTimeout(Duration readTimeout) {
        this.readTimeout = readTimeout;
        return this;
    }

    public Duration getReadTimeout() {
        return this.readTimeout;
    }

    public ClientBuilder setPingInterval(Duration pingInterval) {
        this.pingInterval = pingInterval;
        return this;
    }

    public Duration getPingInterval() {
        return this.pingInterval;
    }

    public String getKeyStorePassphrase() {
        return keyStorePassphrase;
    }

    public ClientBuilder setKeyStorePassphrase(String keyStorePassphrase) {
        this.keyStorePassphrase = keyStorePassphrase;
        return this;
    }

    public ApiClient build() {
        final ApiClient client = new ApiClient();

        client.setHttpClient(
                client
                        .getHttpClient()
                        .newBuilder()
                        .protocols(protocols)
                        .readTimeout(this.readTimeout)
                        .pingInterval(pingInterval)
                        .build());

        if (basePath != null) {
            if (basePath.endsWith("/")) {
                basePath = basePath.substring(0, basePath.length() - 1);
            }
            client.setBasePath(basePath);
        }

        client.setVerifyingSsl(verifyingSsl);

        if (authentication != null) {
            if (StringUtils.isNotEmpty(keyStorePassphrase)) {
                if (authentication instanceof KubeconfigAuthentication) {
                    if (((KubeconfigAuthentication) authentication).getDelegateAuthentication()
                            instanceof ClientCertificateAuthentication) {
                        ((ClientCertificateAuthentication)
                                (((KubeconfigAuthentication) authentication).getDelegateAuthentication()))
                                .setPassphrase(keyStorePassphrase);
                    }
                }
            }
            authentication.provide(client);
        }

        // NOTE: this ordering is important.  The API Client re-evaluates the CA certificate every
        // time the SSL info changes, which means that if this comes after the following call
        // you will try to load a certificate with an exhausted InputStream. So setting the CA
        // certificate _has_ to be the last thing that you do related to SSL.
        //
        // TODO: this (imho) is broken in the generate Java Swagger Client code. We should fix it
        // upstream and remove this dependency.
        //
        // TODO: Add a test to ensure that this works correctly...
        if (caCertBytes != null) {
            client.setSslCaCert(new ByteArrayInputStream(caCertBytes));
        }

        return client;
    }
}
