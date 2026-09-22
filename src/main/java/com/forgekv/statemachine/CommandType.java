package com.forgekv.statemachine;

/**
 * All state-changing operations that can be proposed through Raft.
 */
public enum CommandType {
    PUT,
    DELETE,
    CAS,
    CREATE_QUEUE,
    ENQUEUE_JOB,
    CLAIM_JOB,
    ACK_JOB,
    NACK_JOB,
    EXTEND_LEASE,
    REQUEUE_JOB,
    MOVE_TO_DLQ
}
