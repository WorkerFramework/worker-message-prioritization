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

import retrofit.http.Body;
import retrofit.http.DELETE;
import retrofit.http.GET;
import retrofit.http.PUT;
import retrofit.http.Path;

import java.util.List;

public interface ShovelsApi {
    @PUT("/api/parameters/shovel/{vhost}/{name}")
    RetrievedShovel putShovel(@Path("vhost") final String vhost, @Path("name") final String name, @Body final Component<Shovel> shovel);
    
    @GET("/api/shovels/")
    List<RetrievedShovel> getShovels();

    @GET("/api/shovels/vhost/{vhost}/{name}")
    RetrievedShovel getShovel(@Path("vhost") final String vhost, @Path("name") final String name);

    @DELETE("/api/shovels/vhost/{vhost}/{name}/restart")
    RetrievedShovel restartShovel(@Path("vhost") final String vhost, @Path("name") final String name);

    @DELETE("/api/parameters/shovel/{vhost}/{name}")
    RetrievedShovel delete(@Path("vhost") final String vhost, @Path("name") final String name);
}
