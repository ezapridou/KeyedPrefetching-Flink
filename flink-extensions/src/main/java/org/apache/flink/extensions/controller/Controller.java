package org.apache.flink.extensions.controller;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.executiongraph.IntermediateResultPartition;
import org.apache.flink.runtime.scheduler.strategy.ConsumerVertexGroup;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;

import java.util.*;

public class Controller {
    private final boolean lookaheadSelectionActive;
    private final int initialDelay;
    private final int interval;

    public Controller() {
        this.lookaheadSelectionActive = Boolean.parseBoolean(System.getProperty("lookaheadSelectionActive", "false"));
        this.initialDelay = Integer.parseInt(System.getProperty("reconfInitialDelay", "10000"));
        this.interval = Integer.parseInt(System.getProperty("reconfInterval", "10000"));
    }

    public void registerJobToSendControl(ExecutionGraph graph, JobID jobID) {
        String jobIDStr = jobID.toString();

        System.out.println("Parameters: initialDelay: " + initialDelay
                + ", interval: " + interval);

        Pair<HashMap<Integer, HashMap<String, HashSet<String>>>, HashMap<String, ExecutionVertex>> graphAndMapping =
                convertExecutionGraphToWorkerGraph(graph);
        HashMap<Integer, HashMap<String, HashSet<String>>> graphWithMapping = graphAndMapping.getFirst();
        HashMap<String, ExecutionVertex> operatorToExecutionVertexMap = graphAndMapping.getSecond();

        if (!lookaheadSelectionActive) {
            System.out.println("Dynamic lookahead selection is turned off on the JobMaster side. No controller will be instantiated.");
            return;
        }

        KeyedPrefetchingManager keyedPrefetchingManager = new KeyedPrefetchingManager();
        keyedPrefetchingManager.init(jobIDStr, graphWithMapping, operatorToExecutionVertexMap,interval, initialDelay);
    }

    private int getGroupIdFromName(String name) {
        if (name.contains("GID")) {
            String[] temp = name.split("GID");
            String[] temp2 = temp[temp.length - 1].split("-");
            return Integer.parseInt(temp2[0].replace(" ", ""));
        }
        else {
            //assume GID = 0
            return 0;
        }
    }

    private Pair<HashMap<Integer, HashMap<String, HashSet<String>>>, HashMap<String, ExecutionVertex>>
    convertExecutionGraphToWorkerGraph(ExecutionGraph graph) {
        HashMap<Integer, HashMap<String, HashSet<String>>> result = new HashMap<>();
        HashMap<String, ExecutionVertex> mapping = new HashMap<>();

        for (ExecutionJobVertex v : graph.getAllVertices().values()) {
            for (ExecutionVertex w : v.getTaskVertices()) {
                int groupId = getGroupIdFromName(w.getTaskName());
                result.putIfAbsent(groupId, new HashMap<>());
                result.get(groupId).put(
                        w.getTaskName() + "-" + w.getParallelSubtaskIndex(), getAllDownstreamWorkers(w, graph));
                mapping.put(w.getTaskName() + "-" + w.getParallelSubtaskIndex(), w);
            }
        }

        return new Pair<>(result, mapping);
    }

    private HashSet<String> getAllDownstreamWorkers(ExecutionVertex v, ExecutionGraph graph) {
        HashSet<String> result = new HashSet<>();

        for (IntermediateResultPartition partition : v.getProducedPartitions().values()) {
            // Iterate over the groups
            for (ConsumerVertexGroup group : partition.getConsumerVertexGroups()) {
                for (ExecutionVertexID consumerId : group) {
                    // Look up the ExecutionJobVertex (the logical operator)
                    ExecutionJobVertex jobVertex = graph.getJobVertex(consumerId.getJobVertexId());

                    if (jobVertex != null) {
                        // 5. Look up the specific parallel subtask (ExecutionVertex)
                        ExecutionVertex consumerVertex = jobVertex.getTaskVertices()[consumerId.getSubtaskIndex()];

                        // 6. Generate the name
                        String name = consumerVertex.getTaskNameWithSubtaskIndex();
                        result.add(name);
                    }
                }
            }
        }
        return result;
    }

    // Helper class for returning pairs of values
    private static class Pair<T, U> {
        private final T first;
        private final U second;

        public Pair(T first, U second) {
            this.first = first;
            this.second = second;
        }

        public T getFirst() {
            return first;
        }

        public U getSecond() {
            return second;
        }
    }
}
