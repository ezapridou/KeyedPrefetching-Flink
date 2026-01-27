package org.apache.flink.extensions.controller;

import java.util.HashMap;
import java.util.HashSet;

public class GetSelectedLookaheadsControlMessage extends ControlMessage{
    private static final long serialVersionUID = 4L;
    public GetSelectedLookaheadsControlMessage(
            HashMap<String, HashSet<String>> MCS) {
        super(MCS);
    }

}
