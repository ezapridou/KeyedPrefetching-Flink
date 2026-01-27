package org.apache.flink.extensions.controller;

import org.apache.flink.extensions.reconfiguration.ReconfigurableExecutionVertex;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class KeyedPrefetchingManager extends Optimizer{
    protected int initialDelay;
    protected int interval; // in ms
    protected HashSet<Integer>[] groups;
    protected HashMap<String, ResourceID> taskToResourceIDMap;

    private int groupId = 0; // only 1 group

    private int currentLookahead = 1;

    String file = "iteration,query,group,throughputIsolation,throughput\n";

    public void init(String jobID,
                     HashMap<Integer, HashMap<String, HashSet<String>>> graphWithMapping,
                     HashMap<String, ExecutionVertex> operatorToExecutionVertexMap,
                     int interval, int initialDelay) {
        init(jobID, graphWithMapping);
        this.interval = interval;
        this.initialDelay = initialDelay;

        start(operatorToExecutionVertexMap);
    }

    @Override
    public void start(
            Map<String, ExecutionVertex> operatorToExecutionVertexMap) {

        Timer checkForLookaheadChangeTimer = new Timer();
        TimerTask checkLookaheadChangeTask = getRetrieveSelectedLookaheadsTask(operatorToExecutionVertexMap);

        checkForLookaheadChangeTimer.schedule(checkLookaheadChangeTask, initialDelay, interval);
    }

    private TimerTask getRetrieveSelectedLookaheadsTask(Map<String, ExecutionVertex> operatorToExecutionVertexMap) {
        TimerTask retrieveSelectedLookaheadsTask = new TimerTask() {
            private int iteration = 0;
            @Override
            public void run() {
                // map from operator name to TaskManagerLocation
                // getting the task to resource id map in the init function would not work as
                // resources have not yet been assigned
                if (iteration ==0 ) {
                    taskToResourceIDMap = getTaskToResourceIDMap(operatorToExecutionVertexMap);
                }
                iteration++;
                List<Integer> lookaheads = getSelectedLookaheads(groupId, operatorToExecutionVertexMap);
                int selectedLookahead = Integer.MAX_VALUE;
                for (int l : lookaheads) {
                    if (l < selectedLookahead) {
                        selectedLookahead = l;
                    }
                }
                if (selectedLookahead != currentLookahead) {
                    int oldLookahead = currentLookahead;
                    currentLookahead = selectedLookahead;

                    changeLookaheadState(oldLookahead, false, operatorToExecutionVertexMap);
                    changeLookaheadState(currentLookahead, true, operatorToExecutionVertexMap);
                }
            }
        };
        return retrieveSelectedLookaheadsTask;
    }

    private List<Integer> getSelectedLookaheads(
            int groupId, Map<String, ExecutionVertex> operatorToExecutionVertexMap) {
        ArrayList<CompletableFuture<?>> futures = new ArrayList<>();
        Set<String> receivers = getStatefulTasks();

        GetSelectedLookaheadsControlMessage controlMessage =
                new GetSelectedLookaheadsControlMessage(graphWithMapping.get(groupId));

        for (String workerName : receivers) {
            ExecutionVertex worker = operatorToExecutionVertexMap.get(workerName);
            futures.add(((ReconfigurableExecutionVertex) worker).sendControlMessage(controlMessage));
        }

        CompletableFuture<List<Integer>> futureList = CompletableFuture.allOf(
                        futures.toArray(new CompletableFuture[futures.size()]))
                .thenApply(v -> (List<Integer>) futures.stream()
                        .map(CompletableFuture::join)
                        .collect(Collectors.toList()));

        List<Integer> selectedLookaheads;
        try {
            selectedLookaheads = futureList.get();
            String p = ("Received selected lookaheads: \n" );
            for (int stats : selectedLookaheads) {
                p += stats + "\n";
            }
            System.out.println(p);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }

        return selectedLookaheads;
    }

    private void changeLookaheadState(int lookaheadID, boolean active,
                                      Map<String, ExecutionVertex> operatorToExecutionVertexMap) {
        List<CompletableFuture<?>> futures = new ArrayList<>();
        Set<String> receivers = getLookaheadTasks(lookaheadID);

        EnableDisableLookaheadControlMessage controlMessage = new EnableDisableLookaheadControlMessage(
                graphWithMapping.get(groupId), active);

        for (String workerName : receivers) {
            ExecutionVertex worker = operatorToExecutionVertexMap.get(workerName);
            futures.add(((ReconfigurableExecutionVertex) worker).sendControlMessage(controlMessage));
        }
    }

    private Set<String> getStatefulTasks() {
        return getTasksForGroupByName(groupId, "StatefulOp");
    }

    private Set<String> getLookaheadTasks(int lookaheadID) {
        return getTasksForGroupByName(groupId, "Lookahead" + lookaheadID);
    }

    private Set<String> getTasksForGroupByName(int groupId, String name) {
        Set<String> res = new HashSet<>();
        for (String taskName : graphWithMapping.get(groupId).keySet()) {
            if (taskName.contains(name)) {
                res.add(taskName);
            }
        }
        return res;
    }
}
