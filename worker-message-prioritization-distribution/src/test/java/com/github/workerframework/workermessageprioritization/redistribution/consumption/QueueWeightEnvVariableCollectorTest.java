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
package com.github.workerframework.workermessageprioritization.redistribution.consumption;

import com.github.workerframework.workermessageprioritization.redistribution.EnvVariableCollector;
import org.junit.Test;

import java.util.Map;
import java.util.HashMap;

import static org.junit.Assert.assertEquals;

public class QueueWeightEnvVariableCollectorTest {

    @Test
    public void getQueueWeightTest() {

        final Map<String, String> envVariables = new HashMap<>();

        envVariables.put("CAF_QUEUE_PROCESSING_TIME_GOAL_SECONDS", "300");
        envVariables.put("CAF_WMP_KUBERNETES_NAMESPACES", "private");
        envVariables.put("CAF_ADJUST_QUEUE_WEIGHT", "enrichment\\-workflow$,10");
        envVariables.put("CAF_ADJUST_QUEUE_WEIGHT_1", "clynch,0");
        envVariables.put("CAF_MAX_TARGET_QUEUE_LENGTH", "10000000");
        envVariables.put("CAF_ADJUST_QUEUE_WEIGHT_3", "a77777,3");
        envVariables.put("CAF_RABBITMQ_USERNAME", "guest");
        envVariables.put("CAF_ADJUST_QUEUE_WEIGHT_4", "dataprocessing\\-langdetect\\-in,7");
        envVariables.put("CAF_ADJUST_QUEUE_WEIGHT_5", "repository\\-initialization\\-workflow$,0.5");
        envVariables.put("FAST_LANE_LOG_LEVEL", "DEBUG");
        envVariables.put("CAF_RABBITMQ_PASSWORD", "guest");

        final Map<String, String> refinedValues =
                EnvVariableCollector.getQueueWeightEnvVariables(envVariables);

        assertEquals("There are only 5 environment variables which match the " +
                        "CAF_ADJUST_QUEUE_WEIGHT naming convention. Only these should be returned.",
                5, refinedValues.size());
    }

}
