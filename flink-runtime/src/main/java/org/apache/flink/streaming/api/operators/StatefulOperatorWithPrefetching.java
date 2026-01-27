package org.apache.flink.streaming.api.operators;

public interface StatefulOperatorWithPrefetching {
    /*
     * Returns the ID of the selected lookahead.
     */
    public int getSelectedLookaheadID();
}
