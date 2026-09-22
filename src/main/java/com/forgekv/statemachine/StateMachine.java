package com.forgekv.statemachine;

/**
 * Deterministic replicated state machine interface.
 * Commits are applied strictly in increasing log-index order.
 */
public interface StateMachine {

    /**
     * Applies a committed command deterministically to the state machine.
     *
     * @param serializedCommand raw bytes of the encoded Command
     * @return serialized execution result for returning to client / future completion
     */
    byte[] apply(byte[] serializedCommand);
}
