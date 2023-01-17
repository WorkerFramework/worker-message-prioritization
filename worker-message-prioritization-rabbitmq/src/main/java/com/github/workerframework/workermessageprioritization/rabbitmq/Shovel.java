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
