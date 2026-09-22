package com.forgekv.queue;

/**
 * Status of a job in the durable queue.
 */
public enum JobStatus {
    READY,
    PROCESSING,
    COMPLETED,
    DEAD
}
