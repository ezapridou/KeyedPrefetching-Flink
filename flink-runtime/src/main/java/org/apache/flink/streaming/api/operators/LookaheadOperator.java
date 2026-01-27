package org.apache.flink.streaming.api.operators;

public interface LookaheadOperator {
    public void setState(boolean active);
}
