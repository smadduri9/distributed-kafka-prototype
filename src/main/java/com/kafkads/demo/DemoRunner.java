package com.kafkads.demo;

/**
 * Simple runner for the feature demonstrations.
 * Can be run directly or via Gradle.
 */
public class DemoRunner {
    public static void main(String[] args) {
        try {
            FeatureDemo.main(args);
        } catch (Exception e) {
            System.err.println("Error running demo: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
}

