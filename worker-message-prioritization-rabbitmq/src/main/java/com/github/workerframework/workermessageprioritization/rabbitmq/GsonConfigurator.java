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
package com.github.workerframework.workermessageprioritization.rabbitmq;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.ToNumberPolicy;

/**
 * Provides custom configuration for Gson.
 */
@javax.ws.rs.ext.Provider
final class GsonConfigurator implements javax.ws.rs.ext.ContextResolver<Gson>
{
    private final Gson mapper;

    public GsonConfigurator()
    {
     // By default, GSON will read all numbers as Float/Double, so a response from RabbitMQ containing a property like this:
        //
        // "x-max-priority": 5
        //
        // will be read as:
        //
        // "x-max-priority": 5.0
        //
        // This is not acceptable, as RabbitMQ has defined this particular property as an integer, and will not accept a double if we
        // then try to create a staging queue with this property.
        //
        // As such, we are customizing GSON to use the LONG_OR_DOUBLE number policy, which ensures numbers will be read as Long or Double
        // values depending on how JSON numbers are represented.
        mapper = new GsonBuilder().setObjectToNumberStrategy(ToNumberPolicy.LONG_OR_DOUBLE).create();
    }

    @Override
    public Gson getContext(Class<?> type)
    {
        return mapper;
    }
}
