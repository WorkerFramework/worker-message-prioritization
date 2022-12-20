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

import com.google.gson.Gson;
import com.squareup.okhttp.OkHttpClient;
import retrofit.ErrorHandler;
import retrofit.RestAdapter;
import retrofit.RetrofitError;
import retrofit.client.OkClient;
import retrofit.converter.GsonConverter;


import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.concurrent.TimeUnit;

public class RabbitManagementApi <T> {

    private static final int READ_TIMEOUT_SECONDS = 10;
    private static final int CONNECT_TIMEOUT_SECONDS = 10;
    
    private T api;

    public RabbitManagementApi(final Class<T> apiType, final String endpoint, final String user, 
                               final String password) {

        final OkHttpClient okHttpClient = new OkHttpClient();
        okHttpClient.setReadTimeout(READ_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        okHttpClient.setConnectTimeout(CONNECT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        final RestAdapter.Builder restAdapterBuilder
                = new RestAdapter.Builder().setEndpoint(endpoint).setClient(new OkClient(okHttpClient))
                .setConverter(new GsonConverter(new Gson()));
        restAdapterBuilder.setRequestInterceptor(requestFacade -> {
            final String credentials = user + ":" + password;
            final String authorizationHeaderValue
                    = "Basic " + Base64.getEncoder().encodeToString(credentials.getBytes(StandardCharsets.UTF_8));
            requestFacade.addHeader("Accept", "application/json");
            requestFacade.addHeader("Authorization", authorizationHeaderValue);
        });
        restAdapterBuilder.setErrorHandler(new RabbitManagementApi.RabbitApiErrorHandler());
        final RestAdapter restAdapter = restAdapterBuilder.build();
        api = restAdapter.create(apiType);
    }
    
    public T getApi() {
        return api;
    }

    private static class RabbitApiErrorHandler implements ErrorHandler
    {
        @Override
        public Throwable handleError(final RetrofitError retrofitError)
        {
            return new RuntimeException("RabbitMQ management API error " + retrofitError.getResponse().toString(), retrofitError);
        }
    }

}
