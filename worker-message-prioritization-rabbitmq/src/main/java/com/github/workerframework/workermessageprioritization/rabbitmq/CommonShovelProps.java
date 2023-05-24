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

public abstract class CommonShovelProps
{
    @SerializedName(value = "src-queue", alternate = "src_queue")
    private String srcQueue;
    @SerializedName(value = "dest-queue", alternate = "dest_queue")
    private String destQueue;

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

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("srcQueue", srcQueue)
            .add("destQueue", destQueue)
            .toString();
    }
}
