/*
 * Copyright 2022-2022 Micro Focus or one of its affiliates.
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

import com.google.gson.annotations.SerializedName;

public class Shovel {
    
    @SerializedName(value = "src-uri", alternate = "src_uri")
    private String srcUri;
    @SerializedName(value = "dest-uri", alternate = "dest_uri")
    private String destUri;

    @SerializedName(value = "ack-mode", alternate = "ack_mode")
    private String ackMode;
    @SerializedName(value = "src-delete-after", alternate = "src_delete_after")
    private Object srcDeleteAfter;
    @SerializedName(value = "src-queue", alternate = "src_queue")
    private String srcQueue;
    @SerializedName(value = "dest-queue", alternate = "dest_queue")
    private String destQueue;

    public String getAckMode() {
        return ackMode;
    }

    public void setAckMode(String ackMode) {
        this.ackMode = ackMode;
    }

    public Object getSrcDeleteAfter() {
        return srcDeleteAfter;
    }

    public void setSrcDeleteAfter(Object srcDeleteAfter) {
        this.srcDeleteAfter = srcDeleteAfter;
    }

    public String getSrcQueue() {
        return srcQueue;
    }

    public void setSrcQueue(String srcQueue) {
        this.srcQueue = srcQueue;
    }

    public String getDestQueue() {
        return destQueue;
    }

    public void setDestQueue(String destQueue) {
        this.destQueue = destQueue;
    }

    public String getSrcUri() {
        return srcUri;
    }

    public void setSrcUri(String srcUri) {
        this.srcUri = srcUri;
    }

    public String getDestUri() {
        return destUri;
    }

    public void setDestUri(String destUri) {
        this.destUri = destUri;
    }
    
}
