package com.microfocus.apollo.worker.prioritization.management;

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
