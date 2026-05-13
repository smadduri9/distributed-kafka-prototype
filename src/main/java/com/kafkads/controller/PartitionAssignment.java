package com.kafkads.controller;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Handles partition assignment logic for topics.
 */
public class PartitionAssignment {
    
    /**
     * Assigns partitions to brokers using a round-robin strategy.
     */
    public static List<MetadataManager.PartitionAssignment> assignPartitions(
            String topicName, int numPartitions, int replicationFactor, 
            List<MetadataManager.BrokerMetadata> availableBrokers) {
        
        if (availableBrokers.isEmpty()) {
            throw new IllegalArgumentException("No available brokers for assignment");
        }
        
        if (replicationFactor > availableBrokers.size()) {
            throw new IllegalArgumentException(
                "Replication factor (" + replicationFactor + ") exceeds number of brokers (" + 
                availableBrokers.size() + ")");
        }
        
        List<MetadataManager.PartitionAssignment> assignments = new ArrayList<>();
        List<Integer> brokerIds = new ArrayList<>();
        for (MetadataManager.BrokerMetadata broker : availableBrokers) {
            brokerIds.add(broker.getBrokerId());
        }
        
        // Round-robin assignment
        for (int partitionId = 0; partitionId < numPartitions; partitionId++) {
            int leaderIndex = partitionId % brokerIds.size();
            int leaderBrokerId = brokerIds.get(leaderIndex);
            
            // Select replicas (including leader)
            List<Integer> replicaBrokerIds = new ArrayList<>();
            for (int i = 0; i < replicationFactor; i++) {
                int replicaIndex = (leaderIndex + i) % brokerIds.size();
                replicaBrokerIds.add(brokerIds.get(replicaIndex));
            }
            
            MetadataManager.PartitionAssignment assignment = 
                new MetadataManager.PartitionAssignment(topicName, partitionId, leaderBrokerId, replicaBrokerIds);
            assignments.add(assignment);
        }
        
        return assignments;
    }
    
    /**
     * Reassigns partitions when a broker fails.
     */
    public static List<MetadataManager.PartitionAssignment> reassignPartitions(
            List<MetadataManager.PartitionAssignment> existingAssignments,
            int failedBrokerId,
            List<MetadataManager.BrokerMetadata> availableBrokers) {
        
        List<MetadataManager.PartitionAssignment> newAssignments = new ArrayList<>();
        List<Integer> availableBrokerIds = new ArrayList<>();
        for (MetadataManager.BrokerMetadata broker : availableBrokers) {
            if (broker.getBrokerId() != failedBrokerId) {
                availableBrokerIds.add(broker.getBrokerId());
            }
        }
        
        if (availableBrokerIds.isEmpty()) {
            throw new IllegalArgumentException("No available brokers after failure");
        }
        
        Random random = new Random();
        
        for (MetadataManager.PartitionAssignment assignment : existingAssignments) {
            if (assignment.getLeaderBrokerId() == failedBrokerId || 
                assignment.getReplicaBrokerIds().contains(failedBrokerId)) {
                
                // Need to reassign
                int newLeaderId = assignment.getLeaderBrokerId();
                List<Integer> newReplicas = new ArrayList<>(assignment.getReplicaBrokerIds());
                
                // If leader failed, elect new leader from replicas
                if (assignment.getLeaderBrokerId() == failedBrokerId) {
                    for (Integer replicaId : assignment.getReplicaBrokerIds()) {
                        if (replicaId != failedBrokerId && availableBrokerIds.contains(replicaId)) {
                            newLeaderId = replicaId;
                            break;
                        }
                    }
                    
                    // If no replica available, pick random available broker
                    if (newLeaderId == failedBrokerId) {
                        newLeaderId = availableBrokerIds.get(random.nextInt(availableBrokerIds.size()));
                    }
                }
                
                // Replace failed broker in replica list
                newReplicas.removeIf(id -> id == failedBrokerId);
                while (newReplicas.size() < assignment.getReplicaBrokerIds().size() && 
                       newReplicas.size() < availableBrokerIds.size()) {
                    // Add a new replica
                    for (Integer brokerId : availableBrokerIds) {
                        if (!newReplicas.contains(brokerId)) {
                            newReplicas.add(brokerId);
                            break;
                        }
                    }
                }
                
                MetadataManager.PartitionAssignment newAssignment = 
                    new MetadataManager.PartitionAssignment(
                        assignment.getTopicName(),
                        assignment.getPartitionId(),
                        newLeaderId,
                        newReplicas
                    );
                newAssignments.add(newAssignment);
            } else {
                // Keep existing assignment
                newAssignments.add(assignment);
            }
        }
        
        return newAssignments;
    }
}

