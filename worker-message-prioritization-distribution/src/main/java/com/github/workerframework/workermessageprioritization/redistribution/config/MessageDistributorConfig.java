/*
 * Copyright 2022-2023 Micro Focus or one of its affiliates.
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
package com.github.workerframework.workermessageprioritization.redistribution.config;

import com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class MessageDistributorConfig {

    private static final Logger LOGGER = LoggerFactory.getLogger(MessageDistributorConfig.class);

    private MessageDistributorConfig() {
        
    }

    public static final String RABBITMQ_HOST = "RABBITMQ_HOST";
    public static final String RABBITMQ_PORT = "RABBITMQ_PORT";
    public static final String RABBITMQ_USERNAME = "RABBITMQ_USERNAME";
    public static final String RABBITMQ_PASSWORD = "RABBITMQ_PASSWORD";

    public static final String RABBITMQ_MANAGEMENT_PROTOCOL = "RABBITMQ_MANAGEMENT_PROTOCOL";
    public static final String RABBITMQ_MANAGEMENT_HOST = "RABBITMQ_MANAGEMENT_HOST";
    public static final String RABBITMQ_MANAGEMENT_PORT = "RABBITMQ_MANAGEMENT_PORT";
    public static final String RABBITMQ_MANAGEMENT_USERNAME = "RABBITMQ_MANAGEMENT_USERNAME";
    public static final String RABBITMQ_MANAGEMENT_PASSWORD = "RABBITMQ_MANAGEMENT_PASSWORD";

    public static String getEnv(final String name) {

        final String value = System.getenv(name);

        if (!Strings.isNullOrEmpty(value)) {
            LOGGER.info("Read environment variable: {}={}", name, value);
            return value;
        } else {
            throw new RuntimeException(String.format("The %s environment variable must be supplied, and must not be empty", name));
        }
    }
}
