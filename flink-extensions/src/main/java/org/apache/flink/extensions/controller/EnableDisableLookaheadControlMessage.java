package org.apache.flink.extensions.controller;

import java.util.HashMap;
import java.util.HashSet;

public class EnableDisableLookaheadControlMessage extends ControlMessage {
    private static final long serialVersionUID = 3L;

    public final boolean active;

    public EnableDisableLookaheadControlMessage(
            HashMap<String, HashSet<String>> MCS, boolean active) {
        super(MCS);
        this.active = active;
    }
}
