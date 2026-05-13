package com.kafkads.security;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Manages Access Control Lists (ACLs) for topic and operation permissions.
 */
public class ACLManager {
    private static final Logger logger = LoggerFactory.getLogger(ACLManager.class);
    
    public enum Operation {
        READ,
        WRITE,
        CREATE,
        DELETE,
        ALTER
    }
    
    public enum ResourceType {
        TOPIC,
        GROUP,
        CLUSTER
    }
    
    public static class ACL {
        private final String principal;
        private final ResourceType resourceType;
        private final String resourceName;
        private final Operation operation;
        private final boolean allow;
        
        public ACL(String principal, ResourceType resourceType, String resourceName, 
                  Operation operation, boolean allow) {
            this.principal = principal;
            this.resourceType = resourceType;
            this.resourceName = resourceName;
            this.operation = operation;
            this.allow = allow;
        }
        
        public String getPrincipal() { return principal; }
        public ResourceType getResourceType() { return resourceType; }
        public String getResourceName() { return resourceName; }
        public Operation getOperation() { return operation; }
        public boolean isAllow() { return allow; }
    }
    
    private final ConcurrentMap<String, Set<ACL>> acls = new ConcurrentHashMap<>();
    private volatile boolean enabled = false;
    
    public ACLManager(boolean enabled) {
        this.enabled = enabled;
    }
    
    /**
     * Checks if a principal has permission for an operation on a resource.
     */
    public boolean checkPermission(String principal, ResourceType resourceType, 
                                  String resourceName, Operation operation) {
        if (!enabled) {
            return true; // ACL disabled, allow all
        }
        
        String key = resourceType + ":" + resourceName;
        Set<ACL> resourceAcls = acls.get(key);
        
        if (resourceAcls == null || resourceAcls.isEmpty()) {
            // No ACLs defined, deny by default
            logger.debug("No ACLs found for resource: {}, denying access", key);
            return false;
        }
        
        // Check for matching ACL
        for (ACL acl : resourceAcls) {
            if (acl.getPrincipal().equals(principal) && 
                acl.getOperation() == operation) {
                return acl.isAllow();
            }
        }
        
        // No matching ACL, deny by default
        logger.debug("No matching ACL for principal: {}, resource: {}, operation: {}", 
            principal, key, operation);
        return false;
    }
    
    /**
     * Adds an ACL rule.
     */
    public void addACL(ACL acl) {
        String key = acl.getResourceType() + ":" + acl.getResourceName();
        acls.computeIfAbsent(key, k -> ConcurrentHashMap.newKeySet()).add(acl);
        logger.info("ACL added: principal={}, resource={}, operation={}, allow={}", 
            acl.getPrincipal(), key, acl.getOperation(), acl.isAllow());
    }
    
    /**
     * Removes an ACL rule.
     */
    public void removeACL(ACL acl) {
        String key = acl.getResourceType() + ":" + acl.getResourceName();
        Set<ACL> resourceAcls = acls.get(key);
        if (resourceAcls != null) {
            resourceAcls.remove(acl);
        }
    }
    
    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }
    
    public boolean isEnabled() {
        return enabled;
    }
}

