package com.forgekv.raft;

public class NotLeaderException extends RuntimeException {
    private final String leaderId;

    public NotLeaderException(String leaderId) {
        super("Not current leader. Leader hint: " + (leaderId != null ? leaderId : "UNKNOWN"));
        this.leaderId = leaderId;
    }

    public String getLeaderId() {
        return leaderId;
    }
}
