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
package com.github.workerframework.workermessageprioritization.redistribution.shovel;

import com.squareup.okhttp.HttpUrl;

public final class NodeSpecificRabbitMqMgmtUrlBuilder
{
    private NodeSpecificRabbitMqMgmtUrlBuilder()
    {
    }

    // Given node:              rabbit@baltra-cent02.aspensb.local
    // Given rabbitMQMgmtUrl:   https://baltra-rabbitmq.aspensb.local:15672
    // Returns:                 https://baltra-cent02.aspensb.local:15672/
    public static String buildNodeSpecificRabbitMqMgmtUrl(final String node, final String rabbitMQMgmtUrl)
    {
        final String nodeSpecificHost = node.substring(node.indexOf('@') + 1);

        return HttpUrl.parse(rabbitMQMgmtUrl)
                .newBuilder()
                .host(nodeSpecificHost)
                .build()
                .toString();
    }
}
