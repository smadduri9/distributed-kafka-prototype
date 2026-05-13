package com.kafkads.util;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * Utility class for concurrent operations.
 */
public class ConcurrentUtils {
    
    /**
     * Creates a thread pool with the specified number of threads.
     */
    public static ExecutorService createThreadPool(int threads, String namePrefix) {
        return Executors.newFixedThreadPool(threads, r -> {
            Thread t = new Thread(r);
            t.setName(namePrefix + "-" + t.getId());
            t.setDaemon(true);
            return t;
        });
    }
    
    /**
     * Executes a task asynchronously and returns a CompletableFuture.
     */
    public static <T> CompletableFuture<T> executeAsync(Supplier<T> task, ExecutorService executor) {
        return CompletableFuture.supplyAsync(task, executor);
    }
    
    /**
     * Shuts down an ExecutorService gracefully.
     */
    public static void shutdownExecutor(ExecutorService executor, long timeoutSeconds) {
        if (executor == null) {
            return;
        }
        
        executor.shutdown();
        try {
            if (!executor.awaitTermination(timeoutSeconds, TimeUnit.SECONDS)) {
                executor.shutdownNow();
                if (!executor.awaitTermination(timeoutSeconds, TimeUnit.SECONDS)) {
                    System.err.println("Executor did not terminate");
                }
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}

