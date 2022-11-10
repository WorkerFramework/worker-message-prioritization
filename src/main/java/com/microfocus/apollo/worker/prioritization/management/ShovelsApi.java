package com.microfocus.apollo.worker.prioritization.management;

import retrofit.http.Body;
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
}
