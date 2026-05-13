package com.kafkads.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class for error handling and error codes.
 */
public class ErrorHandler {
    private static final Logger logger = LoggerFactory.getLogger(ErrorHandler.class);
    
    public enum ErrorCode {
        NONE(0, "No error"),
        UNKNOWN(-1, "Unknown error"),
        TOPIC_NOT_FOUND(1, "Topic not found"),
        PARTITION_NOT_FOUND(2, "Partition not found"),
        INVALID_REQUEST(3, "Invalid request"),
        LEADER_NOT_AVAILABLE(4, "Leader not available"),
        NOT_LEADER_FOR_PARTITION(5, "Not leader for partition"),
        REPLICATION_NOT_AVAILABLE(6, "Replication not available"),
        OFFSET_OUT_OF_RANGE(7, "Offset out of range"),
        INVALID_OFFSET(8, "Invalid offset"),
        NETWORK_ERROR(9, "Network error"),
        TIMEOUT(10, "Request timeout"),
        COMPRESSION_ERROR(11, "Compression error"),
        DECOMPRESSION_ERROR(12, "Decompression error"),
        SSL_ERROR(13, "SSL error"),
        ACL_DENIED(14, "ACL denied"),
        TRANSACTION_ERROR(15, "Transaction error");
        
        private final int code;
        private final String message;
        
        ErrorCode(int code, String message) {
            this.code = code;
            this.message = message;
        }
        
        public int getCode() {
            return code;
        }
        
        public String getMessage() {
            return message;
        }
    }
    
    /**
     * Logs an error with context information.
     */
    public static void logError(ErrorCode errorCode, String context, Throwable throwable) {
        logger.error("[{}] {} - Context: {}", errorCode.getCode(), errorCode.getMessage(), context, throwable);
    }
    
    /**
     * Logs an error without exception.
     */
    public static void logError(ErrorCode errorCode, String context) {
        logger.error("[{}] {} - Context: {}", errorCode.getCode(), errorCode.getMessage(), context);
    }
    
    /**
     * Creates an error response message.
     */
    public static String createErrorMessage(ErrorCode errorCode) {
        return String.format("ERROR[%d]: %s", errorCode.getCode(), errorCode.getMessage());
    }
}

