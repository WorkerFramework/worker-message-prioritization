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

import java.security.cert.CertificateParsingException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLSession;

/**
 * A Java implementation of <a href="https://github.com/square/okhttp/blob/parent-5.0.0-alpha.14/okhttp/src/main/kotlin/okhttp3/internal/tls/OkHostnameVerifier.kt">OkHostnameVerifier.kt</a>.
 */
final class Rfc2818HostnameVerifier implements HostnameVerifier
{
    private static final int ALT_DNS_NAME = 2;
    private static final int ALT_IPA_NAME = 7;

    @Override
    public boolean verify(final String host, final SSLSession session)
    {
        try {
            final X509Certificate certificate = (X509Certificate)session.getPeerCertificates()[0];
            return verify(host, certificate);
        } catch (final SSLException e) {
            return false;
        }
    }

    public boolean verify(final String host, final X509Certificate certificate)
    {
        if (isIpAddress(host)) {
            return verifyIpAddress(host, certificate);
        } else {
            return verifyHostname(host, certificate);
        }
    }

    private boolean verifyIpAddress(final String ipAddress, final X509Certificate certificate)
    {
        final String canonicalIpAddress = canonicalizeHost(ipAddress);
        for (final String altName : getSubjectAltNames(certificate, ALT_IPA_NAME)) {
            if (canonicalIpAddress.equals(canonicalizeHost(altName))) {
                return true;
            }
        }
        return false;
    }

    private boolean verifyHostname(final String hostname, final X509Certificate certificate)
    {
        final String normalizedHostname = hostname.toLowerCase(Locale.US);
        for (final String altName : getSubjectAltNames(certificate, ALT_DNS_NAME)) {
            if (verifyHostname(normalizedHostname, altName.toLowerCase(Locale.US))) {
                return true;
            }
        }
        return false;
    }

    private boolean verifyHostname(final String hostname, final String pattern) {
        if (hostname == null || pattern == null || hostname.isEmpty() || pattern.isEmpty()) {
            return false;
        }

        final String normalizedHostname = hostname.endsWith(".") ? hostname : hostname + ".";
        final String normalizedPattern = pattern.endsWith(".") ? pattern : pattern + ".";

        if (!normalizedPattern.contains("*")) {
            return normalizedHostname.equals(normalizedPattern);
        }

        if (!normalizedPattern.startsWith("*.") || normalizedPattern.indexOf('*', 1) != -1) {
            return false;
        }

        final String suffix = normalizedPattern.substring(1);
        if (!normalizedHostname.endsWith(suffix)) {
            return false;
        }

        final int suffixStartIndex = normalizedHostname.length() - suffix.length();
        return suffixStartIndex == 0 || normalizedHostname.lastIndexOf('.', suffixStartIndex - 1) == -1;
    }

    private boolean isIpAddress(final String host)
    {
        return host != null && (host.contains(":") || host.matches("\\d+\\.\\d+\\.\\d+\\.\\d+"));
    }

    private String canonicalizeHost(final String host)
    {
        if (host == null) return null;
        return host.endsWith(".") ? host : host + ".";
    }

    private List<String> getSubjectAltNames(final X509Certificate certificate, final int type)
    {
        final List<String> altNames = new ArrayList<>();
        try {
            final Collection<List<?>> sanEntries = certificate.getSubjectAlternativeNames();
            if (sanEntries == null) return altNames;
            for (final List<?> sanEntry : sanEntries) {
                if (sanEntry == null || sanEntry.size() < 2) continue;
                if (((Integer)sanEntry.get(0)) == type) {
                    altNames.add((String)sanEntry.get(1));
                }
            }
        } catch (final CertificateParsingException ignored) {
            // Ignored
        }
        return altNames;
    }
}
