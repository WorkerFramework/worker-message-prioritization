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
package com.github.workerframework.workermessageprioritization.redistribution.consumption;

import com.github.workerframework.workermessageprioritization.redistribution.EnvVariableCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.List;
import java.util.HashMap;
import java.util.Collections;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class StagingQueueWeightSettingsProvider {
    private static final Logger FAST_LANE_LOGGER = LoggerFactory.getLogger("FAST_LANE");
    // This matches env variable strings with regex followed by comma and number.
    private static final String ADJUST_QUEUE_WEIGHT_STRING_MATCHER = "[^.]*,(?!.*,)(0|[1-9]\\d*)?(\\.\\d+)?(?<=\\d)$";
    private static final Pattern ADJUST_QUEUE_WEIGHT_STRING_PATTERN = Pattern.compile(ADJUST_QUEUE_WEIGHT_STRING_MATCHER);

    public final Map<String, Double> getStagingQueueWeights(List<String> stagingQueueNames) {

        final Map<String, Double> stagingQueueWeights = new HashMap<>();

        final Map<String, String> envVariables =
                EnvVariableCollector.getQueueWeightEnvVariables(EnvVariableCollector.getEnvVariables());

        final Map<Pattern, Double> regexToWeightMap = new HashMap<>();

        // Loop over the environment variable strings added to get regex and weight
        for (final Map.Entry<String, String> regexWeightString : envVariables.entrySet()) {
            final Matcher matcher = ADJUST_QUEUE_WEIGHT_STRING_PATTERN.matcher(regexWeightString.getValue());

            // Confirm all strings passed through match the required format.
            if (!matcher.matches()) {
                throw new IllegalArgumentException(String.format("Illegal format for CAF_ADJUST_QUEUE_WEIGHT string: '%s'. " +
                        "Please ensure there are no spaces in the string, negative numbers or unnecessary zeros preceding " +
                        "the weight value.",
                        regexWeightString.getValue()));
            }
            final String[] regexPattern = regexWeightString.getValue().split(",(?!.*,)");
            regexToWeightMap.put(Pattern.compile(regexPattern[0]), Double.parseDouble(regexPattern[1]));
        }

        for (final String stagingQueue : stagingQueueNames) {

            final Map<Integer, Double> matchLengthToWeight = new HashMap<>();

            // Find the regex strings that match the staging queue
            for (final Map.Entry<Pattern, Double> regexAndWeight : regexToWeightMap.entrySet()) {
                final Matcher matcher = regexAndWeight.getKey().matcher(stagingQueue);

                while (matcher.find()) {
                    // Map the length of the string match to the weight that has been matched

                    // If there are 2 regex patterns that match the same length of string but provide different weights,
                    // then choose weight that is larger.
                    matchLengthToWeight.compute(matcher.group().length(),
                            (key, val) -> (val == null ? regexAndWeight.getValue() : Math.max(regexAndWeight.getValue(), val)));

                }
            }

            // if there are no regex patterns that match the staging queue, set to the default weight of 1.
            if (matchLengthToWeight.isEmpty()) {
                stagingQueueWeights.put(stagingQueue, 1D);
            } else {
                // Find the longest matching string, and apply that weight
                final int maxKey = Collections.max(matchLengthToWeight.keySet());
                final double weight = matchLengthToWeight.get(maxKey);
                FAST_LANE_LOGGER.debug("{}: weight has been adjusted to: {}", stagingQueue, weight);
                stagingQueueWeights.put(stagingQueue, weight);
            }
        }

        return stagingQueueWeights;
    }
}
