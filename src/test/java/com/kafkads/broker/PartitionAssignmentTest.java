package com.kafkads.broker;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PartitionAssignmentTest {

    @Test
    void roundRobinSpreadsReplicasAndPreferredLeaders() {
        PartitionAssignment a = PartitionAssignment.roundRobin(Set.of(30, 10, 20, 40), 6, 3);
        assertEquals(6, a.partitionCount());
        assertEquals(List.of(10, 20, 30), a.replicas(0));
        assertEquals(List.of(20, 30, 40), a.replicas(1));
        assertEquals(List.of(30, 40, 10), a.replicas(2));
        assertEquals(List.of(40, 10, 20), a.replicas(3));
        assertEquals(List.of(10, 20, 30), a.replicas(4));
        assertEquals(10, a.preferredLeader(0));
        assertEquals(40, a.preferredLeader(3));
        assertEquals(List.of(0, 2, 3, 4), a.partitionsOf(10));
        assertTrue(a.isReplica(1, 40));
        assertFalse(a.isReplica(1, 10));
        assertFalse(a.isReplica(99, 10));
    }

    @Test
    void everyBrokerComputesTheSameAssignmentRegardlessOfInputOrder() {
        assertEquals(PartitionAssignment.roundRobin(List.of(1, 2, 3), 5, 2).toString(),
                PartitionAssignment.roundRobin(List.of(3, 1, 2), 5, 2).toString());
    }

    @Test
    void replicationFactorIsCappedAtTheBrokerCount() {
        PartitionAssignment a = PartitionAssignment.roundRobin(Set.of(1, 2), 3, 5);
        assertEquals(List.of(1, 2), a.replicas(0));
        assertEquals(List.of(2, 1), a.replicas(1));
        PartitionAssignment single = PartitionAssignment.roundRobin(Set.of(7), 2, 3);
        assertEquals(List.of(7), single.replicas(1));
    }

    @Test
    void explicitAssignmentIsParsedAndValidated() {
        PartitionAssignment a = PartitionAssignment.parse("0:3,1 ; 1:2", Set.of(1, 2, 3), 2);
        assertEquals(List.of(3, 1), a.replicas(0));
        assertEquals(3, a.preferredLeader(0));
        assertEquals(List.of(2), a.replicas(1));

        assertThrows(IllegalArgumentException.class,
                () -> PartitionAssignment.parse("0:1,2", Set.of(1, 2, 3), 2), "partition 1 missing");
        assertThrows(IllegalArgumentException.class,
                () -> PartitionAssignment.parse("0:1,9;1:2", Set.of(1, 2, 3), 2), "unknown broker");
        assertThrows(IllegalArgumentException.class,
                () -> PartitionAssignment.parse("0:1,1;1:2", Set.of(1, 2, 3), 2), "duplicate replica");
        assertThrows(IllegalArgumentException.class,
                () -> PartitionAssignment.parse("0:1;0:2", Set.of(1, 2, 3), 2), "partition listed twice");
        assertThrows(IllegalArgumentException.class, () -> a.replicas(5));
    }
}
