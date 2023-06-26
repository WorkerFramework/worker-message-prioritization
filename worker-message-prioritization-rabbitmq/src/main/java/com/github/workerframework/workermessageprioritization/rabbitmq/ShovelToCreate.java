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

import com.google.common.base.MoreObjects;
import com.google.gson.annotations.SerializedName;

public class ShovelToCreate extends CommonShovelProps
{
    // "src_uri" and "dest-uri" are declared here and not in CommonShovelProps (and so not inherited by RetrievedShovel) because although
    // these properties are returned in the RabbitMQ GET response, they do not contain the RabbitMQ username.
    // For example, a Shovel with a src-uri of "amqp://darwin_user@/%2F" is returned as this in the GET response: "amqp:///%2F"
    // As such, these are not a true reflection of the actual src-uri and dest-uri of the shovel, so it's best not to include them in
    // RetrievedShovel to avoid any confusion.
    @SerializedName(value = "src-uri", alternate = "src_uri")
    private String srcUri;
    @SerializedName(value = "dest-uri", alternate = "dest_uri")
    private String destUri;
    @SerializedName(value = "ack-mode", alternate = "ack_mode")
    private String ackMode;
    @SerializedName(value = "src-delete-after", alternate = "src_delete_after")
    private Object srcDeleteAfter;
    @SerializedName(value = "src-prefetch-count", alternate = "src_prefetch_count")
    private Object srcPrefetchCount;

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

    public Object getSrcPrefetchCount() {
        return srcPrefetchCount;
    }

    public void setSrcPrefetchCount(Object srcPrefetchCount) {
        this.srcPrefetchCount = srcPrefetchCount;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("srcUri", srcUri)
            .add("destUri", destUri)
            .add("ackMode", ackMode)
            .add("srcDeleteAfter", srcDeleteAfter)
            .add("srcQueue", getSrcQueue())
            .add("destQueue", getDestQueue())
            .add("srcPrefetchCount", getSrcPrefetchCount())
            .toString();
    }
}
