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
package com.github.workerframework.workermessageprioritization.redistribution;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class EnvVariableCollector {
    private static final String regexEnvMatcher = "^CAF_ADJUST_QUEUE_WEIGHT.*";
    private static final Pattern pattern = Pattern.compile(regexEnvMatcher);

    public static Map<String, String> getEnvVariables() {

        return System.getenv();

    }

    public static Map<String, String> getQueueWeightEnvVariables(Map<String, String> envVariables) {

        final Map<String, String> queueWeightEnvVariablesToFind = new HashMap<>();

        for (final Map.Entry<String, String> entry : envVariables.entrySet()) {
            final Matcher matcher = pattern.matcher(entry.getKey());

            if (matcher.matches()) {
                queueWeightEnvVariablesToFind.put(entry.getKey(), entry.getValue());
            }
        }

        return queueWeightEnvVariablesToFind;
    }
}
