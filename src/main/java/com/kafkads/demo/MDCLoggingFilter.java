package com.kafkads.demo;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.filter.Filter;
import ch.qos.logback.core.spi.FilterReply;

/**
 * Filter to route logs to specific files based on MDC context.
 */
public class MDCLoggingFilter extends Filter<ILoggingEvent> {
    private String nodeId;
    private String brokerId;
    
    public void setNodeId(String nodeId) {
        this.nodeId = nodeId;
    }
    
    public void setBrokerId(String brokerId) {
        this.brokerId = brokerId;
    }
    
    @Override
    public FilterReply decide(ILoggingEvent event) {
        String eventNodeId = event.getMDCPropertyMap().get("nodeId");
        String eventBrokerId = event.getMDCPropertyMap().get("brokerId");
        
        if (nodeId != null && nodeId.equals(eventNodeId)) {
            return FilterReply.ACCEPT;
        }
        
        if (brokerId != null && brokerId.equals(eventBrokerId)) {
            return FilterReply.ACCEPT;
        }
        
        // If no MDC is set, accept (for backward compatibility)
        if (eventNodeId == null && eventBrokerId == null) {
            return FilterReply.ACCEPT;
        }
        
        return FilterReply.DENY;
    }
}

