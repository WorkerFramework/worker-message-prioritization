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
package com.github.workerframework.workermessageprioritization.rabbitmq;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.ToNumberPolicy;
import com.squareup.okhttp.OkHttpClient;
import retrofit.ErrorHandler;
import retrofit.RestAdapter;
import retrofit.RetrofitError;
import retrofit.client.OkClient;
import retrofit.client.Response;
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
                .setConverter(new GsonConverter(createGson()));
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

    private static Gson createGson() {

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

        return new GsonBuilder()
            .setObjectToNumberStrategy(ToNumberPolicy.LONG_OR_DOUBLE)
            .create();
    }

    private static class RabbitApiErrorHandler implements ErrorHandler
    {
        @Override
        public Throwable handleError(final RetrofitError retrofitError)
        {
            final Response response = retrofitError.getResponse();
            final String responseStatus = response != null ? String.valueOf(response.getStatus()) : null;
            final String responseReason = response != null ? response.getReason() : null;
            final String responseBody =  response != null && response.getBody() != null ? response.getBody().toString() : null;

            final String errorMessage = String.format(
                    "RabbitMQ management API error: " +
                            "requestUrl=%s, " +
                            "responseStatus=%s, " +
                            "responseReason=%s, " +
                            "responseBody=%s, " +
                            "kind=%s, " +
                            "successType=%s, " +
                            "cause=%s",
                    retrofitError.getUrl(),
                    responseStatus,
                    responseReason,
                    responseBody,
                    retrofitError.getKind(),
                    retrofitError.getSuccessType(),
                    retrofitError.getCause());

            return new RuntimeException(errorMessage, retrofitError);
        }
    }

}
