/*
 * Copyright 2022-2022 Micro Focus or one of its affiliates.
 *
 * The only warranties for products and services of Micro Focus and its
 * affiliates and licensors ("Micro Focus") are set forth in the express
 * warranty statements accompanying such products and services. Nothing
 * herein should be construed as constituting an additional warranty.
 * Micro Focus shall not be liable for technical or editorial errors or
 * omissions contained herein. The information contained herein is subject
 * to change without notice.
 *
 * Contains Confidential Information. Except as specifically indicated
 * otherwise, a valid license is required for possession, use or copying.
 * Consistent with FAR 12.211 and 12.212, Commercial Computer Software,
 * Computer Software Documentation, and Technical Data for Commercial
 * Items are licensed to the U.S. Government under vendor's standard
 * commercial license.
 */
package com.microfocus.apollo.worker.prioritization.management;

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
