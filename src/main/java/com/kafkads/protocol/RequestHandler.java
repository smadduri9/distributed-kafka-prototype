package com.kafkads.protocol;

import com.kafkads.broker.Broker;
import com.kafkads.broker.Partition;
import com.kafkads.broker.Topic;
import com.kafkads.broker.storage.LogSegment;
import com.kafkads.util.ErrorHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Handles protocol requests and routes them to appropriate broker operations.
 */
public class RequestHandler {
    private static final Logger logger = LoggerFactory.getLogger(RequestHandler.class);
    
    private final Broker broker;
    
    public RequestHandler(Broker broker) {
        this.broker = broker;
    }
    
    /**
     * Handles a protocol request and returns a response.
     */
    public byte[] handleRequest(RequestParser.ProtocolRequest request) {
        try {
            RequestParser.RequestType requestType = request.getRequestType();
            
            switch (requestType) {
                case CREATE_TOPIC:
                    return handleCreateTopic(request);
                case PRODUCE:
                    return handleProduce(request);
                case FETCH:
                    return handleFetch(request);
                case METADATA:
                    return handleMetadata(request);
                case HEARTBEAT:
                    return handleHeartbeat(request);
                default:
                    logger.warn("Unknown request type: {}", requestType);
                    return ResponseBuilder.buildErrorResponse(
                        ErrorHandler.ErrorCode.INVALID_REQUEST,
                        "Unknown request type: " + requestType
                    );
            }
        } catch (Exception e) {
            logger.error("Error handling request: {}", e.getMessage(), e);
            return ResponseBuilder.buildErrorResponse(
                ErrorHandler.ErrorCode.UNKNOWN,
                "Internal error: " + e.getMessage()
            );
        }
    }
    
    private byte[] handleCreateTopic(RequestParser.ProtocolRequest request) {
        try {
            RequestParser.CreateTopicRequest createRequest = RequestParser.parseCreateTopic(request.getBody());
            
            boolean success = broker.createTopic(
                createRequest.getTopicName(),
                createRequest.getNumPartitions(),
                createRequest.getReplicationFactor()
            );
            
            if (success) {
                return ResponseBuilder.buildCreateTopicResponse(true, null);
            } else {
                return ResponseBuilder.buildCreateTopicResponse(false, "Topic already exists or invalid parameters");
            }
        } catch (Exception e) {
            logger.error("Error creating topic: {}", e.getMessage(), e);
            return ResponseBuilder.buildErrorResponse(
                ErrorHandler.ErrorCode.INVALID_REQUEST,
                "Failed to create topic: " + e.getMessage()
            );
        }
    }
    
    private byte[] handleProduce(RequestParser.ProtocolRequest request) {
        try {
            RequestParser.ProduceRequest produceRequest = RequestParser.parseProduce(request.getBody());
            
            long offset = broker.produce(
                produceRequest.getTopicName(),
                produceRequest.getPartitionId(),
                produceRequest.getMessage()
            );
            
            return ResponseBuilder.buildProduceResponse(offset, ErrorHandler.ErrorCode.NONE);
        } catch (IllegalArgumentException e) {
            logger.warn("Produce request failed: {}", e.getMessage());
            return ResponseBuilder.buildProduceResponse(-1, ErrorHandler.ErrorCode.TOPIC_NOT_FOUND);
        } catch (Exception e) {
            logger.error("Error producing message: {}", e.getMessage(), e);
            return ResponseBuilder.buildProduceResponse(-1, ErrorHandler.ErrorCode.UNKNOWN);
        }
    }
    
    private byte[] handleFetch(RequestParser.ProtocolRequest request) {
        try {
            RequestParser.FetchRequest fetchRequest = RequestParser.parseFetch(request.getBody());
            
            List<LogSegment.MessageRecord> messages = broker.fetch(
                fetchRequest.getTopicName(),
                fetchRequest.getPartitionId(),
                fetchRequest.getOffset(),
                Math.min(fetchRequest.getMaxBytes() / 100, 100) // Rough estimate for max messages
            );
            
            if (messages == null) {
                return ResponseBuilder.buildFetchResponse(
                    new ArrayList<>(),
                    ErrorHandler.ErrorCode.TOPIC_NOT_FOUND
                );
            }
            
            return ResponseBuilder.buildFetchResponse(messages, ErrorHandler.ErrorCode.NONE);
        } catch (IllegalArgumentException e) {
            logger.warn("Fetch request failed: {}", e.getMessage());
            return ResponseBuilder.buildFetchResponse(
                new ArrayList<>(),
                ErrorHandler.ErrorCode.TOPIC_NOT_FOUND
            );
        } catch (Exception e) {
            logger.error("Error fetching messages: {}", e.getMessage(), e);
            return ResponseBuilder.buildFetchResponse(
                new ArrayList<>(),
                ErrorHandler.ErrorCode.UNKNOWN
            );
        }
    }
    
    private byte[] handleMetadata(RequestParser.ProtocolRequest request) {
        try {
            List<String> topics = broker.getAllTopicNames();
            return ResponseBuilder.buildMetadataResponse(topics, ErrorHandler.ErrorCode.NONE);
        } catch (Exception e) {
            logger.error("Error fetching metadata: {}", e.getMessage(), e);
            return ResponseBuilder.buildMetadataResponse(
                new ArrayList<>(),
                ErrorHandler.ErrorCode.UNKNOWN
            );
        }
    }
    
    private byte[] handleHeartbeat(RequestParser.ProtocolRequest request) {
        try {
            // Parse heartbeat request (brokerId)
            int brokerId = broker.getConfig().getBrokerId();
            logger.debug("Heartbeat received from broker: {}", brokerId);
            
            // In a real implementation, this would update the controller
            // For now, just return success
            return ResponseBuilder.buildHeartbeatResponse(true, ErrorHandler.ErrorCode.NONE);
        } catch (Exception e) {
            logger.error("Error handling heartbeat: {}", e.getMessage(), e);
            return ResponseBuilder.buildHeartbeatResponse(false, ErrorHandler.ErrorCode.UNKNOWN);
        }
    }
}

