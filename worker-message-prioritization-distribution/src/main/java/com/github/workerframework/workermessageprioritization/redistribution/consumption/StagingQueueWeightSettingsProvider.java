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

import java.util.ArrayList;
import java.util.Map;
import java.util.List;
import java.util.HashMap;
import java.util.Set;
import java.util.Collections;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class StagingQueueWeightSettingsProvider {
    private static final Logger FAST_LANE_LOGGER = LoggerFactory.getLogger("FAST_LANE");
    ArrayList<String> regexWeightStrings = new ArrayList<>();

    public Map<String, Double> getStagingQueueWeights(List<String> stagingQueueNames) {

        final Map<String, Double> stagingQueueWeights = new HashMap<>();

        final Set<Map.Entry<String, String>> envVariables = EnvVariableCollector.getEnvVariables();

        for (final Map.Entry<String, String> entry : envVariables) {

            // This matches strings with regex followed by comma and number. This is dependent on no whitespace
            final String regexEnvMatcher = "[^.]*,(?!.*,)(0|[1-9]\\d*)?(\\.\\d+)?(?<=\\d)$";

            Pattern pattern = Pattern.compile(regexEnvMatcher);

            Matcher matcher = pattern.matcher(entry.getValue());

            if (matcher.matches()) {
                FAST_LANE_LOGGER.debug("Env variable {} matches the format to alter weight of worker", entry.getValue());
                regexWeightStrings.add(entry.getValue());
            }
        }

        final Map<Pattern, Double> regexToWeightMap = new HashMap<>();

        // Loop over the environment variable strings added to get regex and weight
        for (final String regexWeightString : regexWeightStrings) {
            try {
                final String[] regexPattern = regexWeightString.split(",(?!.*,)");
                regexToWeightMap.put(Pattern.compile(regexPattern[0]), Double.parseDouble(regexPattern[1]));
            } catch (ArrayIndexOutOfBoundsException e) {
                FAST_LANE_LOGGER.error("Invalid Regex string, ensure the format is regex pattern followed by a comma, followed by the " +
                        "weight to be added to the string matching the regex.");
                throw e;
            }
        }

        for (final String stagingQueue : stagingQueueNames) {

            final Map<Integer, Double> matchLengthToWeight = new HashMap<>();

            // Find the regex strings that match the staging queue
            for (final Map.Entry<Pattern, Double> entry : regexToWeightMap.entrySet()) {
                Matcher matcher = entry.getKey().matcher(stagingQueue);

                while (matcher.find()) {
                    int matcherGroupCount = matcher.groupCount();
                    for (int groupCount = 0; groupCount <= matcherGroupCount; groupCount++) {
                        // Map the length of the string match to the weight that has been matched

                        // If there are 2 regex patterns that match the same length of string but provide different weights,
                        // then choose weight that is larger.
                        if (matchLengthToWeight.containsKey(matcher.group(groupCount).length())){
                            matchLengthToWeight.put(matcher.group(groupCount).length(),Math.max(entry.getValue(),
                                    matchLengthToWeight.get(matcher.group(groupCount).length())));
                        }else {
                            matchLengthToWeight.put(matcher.group(groupCount).length(), entry.getValue());
                        }
                    }
                }
            }

            // if there are no regex patterns that match the staging queue, set to the default weight of 1.
            if (matchLengthToWeight.isEmpty()) {
                stagingQueueWeights.put(stagingQueue, 1D);
            } else {
                // Find the longest matching string, and apply that weight
                int maxKey = Collections.max(matchLengthToWeight.keySet());
                double weight = matchLengthToWeight.get(maxKey);
                FAST_LANE_LOGGER.debug("{}: weight has been adjusted to: {}", stagingQueue, weight);
                stagingQueueWeights.put(stagingQueue, weight);
            }
        }

        return stagingQueueWeights;
    }
}
