package org.apache.flink.extensions.reconfiguration;

import org.apache.flink.extensions.controller.ControlMessage;

import java.util.concurrent.CompletableFuture;

/**
 * Interface that makes contained method available to the controller package.
 * TODO: We could also use reflection to call the methods, but I need to check how that works in Scala.
 */
public interface ReconfigurableExecutionVertex {
    CompletableFuture<?> sendControlMessage(ControlMessage message);
}
