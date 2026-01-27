package org.apache.flink.extensions.controller;

import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;

import java.io.Serializable;
import java.util.*;

/**
 * IMPORTANT NOTE:
 * TLDR: This trait must be serializable. Do not include in the state of the class any non-serializable objects.
 * (e.g. the ExecutionVertex object is not serializable)
 * Ofc any objects inheriting this class must be serializable as well.
 *
 * Details: When a control message is sent to a worker, this incurs an RPC call. Flink serializes
 * the control message but also the object graph of the control message. This means that the Optimizer
 * will be serialized as well.
 */
public abstract class Optimizer implements Serializable {

    String jobID = "";
    HashMap<Integer, HashMap<String, HashSet<String>>> graphWithMapping = null;

    protected void init(String jobID,
                        HashMap<Integer, HashMap<String, HashSet<String>>> graphWithMapping) {
        this.jobID = jobID;
        this.graphWithMapping = graphWithMapping;
    }

    public abstract void start(Map<String, ExecutionVertex> operatorToExecutionVertexMap);

    protected HashMap<String, ResourceID> getTaskToResourceIDMap(
            Map<String, ExecutionVertex> operatorToExecutionVertexMap) {
        // map from operator name to TaskManagerLocation
        HashMap<String, ResourceID> taskToResourceIDMap = new HashMap<>();
        for (Map.Entry<String, ExecutionVertex> entry : operatorToExecutionVertexMap.entrySet()) {
            taskToResourceIDMap.put(entry.getKey(), entry.getValue().getCurrentAssignedResourceLocation().getResourceID());
        }
        return taskToResourceIDMap;
    }
}
