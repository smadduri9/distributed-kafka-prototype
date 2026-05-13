package com.kafkads.api;

import com.kafkads.broker.Broker;
import com.kafkads.broker.Topic;
import com.kafkads.broker.storage.LogSegment;
import com.kafkads.config.BrokerConfig;
import com.kafkads.config.ConfigLoader;
import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpExchange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Simple REST API server for the broker.
 */
public class RestApiServer {
    private static final Logger logger = LoggerFactory.getLogger(RestApiServer.class);
    
    private final Broker broker;
    private final HttpServer server;
    private final int port;
    
    public RestApiServer(Broker broker, int port) throws IOException {
        this.broker = broker;
        this.port = port;
        this.server = HttpServer.create(new InetSocketAddress(port), 0);
        setupRoutes();
    }
    
    private void setupRoutes() {
        server.createContext("/api/topics", new TopicsHandler());
        server.createContext("/api/topics/create", new CreateTopicHandler());
        server.createContext("/api/produce", new ProduceHandler());
        server.createContext("/api/fetch", new FetchHandler());
        server.createContext("/api/broker/status", new BrokerStatusHandler());
        server.createContext("/", new StaticFileHandler());
        server.setExecutor(null);
    }
    
    public void start() {
        server.start();
        logger.info("REST API server started on port {}", port);
    }
    
    public void stop() {
        server.stop(0);
        logger.info("REST API server stopped");
    }
    
    private static void sendResponse(HttpExchange exchange, int statusCode, String response) throws IOException {
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.getResponseHeaders().set("Access-Control-Allow-Origin", "*");
        exchange.sendResponseHeaders(statusCode, response.getBytes(StandardCharsets.UTF_8).length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(response.getBytes(StandardCharsets.UTF_8));
        }
    }
    
    private static String readRequestBody(HttpExchange exchange) throws IOException {
        return new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
    }
    
    private class TopicsHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if ("GET".equals(exchange.getRequestMethod())) {
                List<String> topics = broker.getAllTopicNames();
                String topicsJson = topics.stream()
                    .map(t -> "{\"name\":\"" + t + "\"}")
                    .collect(Collectors.joining(","));
                String response = "{\"topics\":[" + topicsJson + "]}";
                sendResponse(exchange, 200, response);
            } else {
                sendResponse(exchange, 405, "{\"error\":\"Method not allowed\"}");
            }
        }
    }
    
    private class CreateTopicHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if ("POST".equals(exchange.getRequestMethod())) {
                String body = readRequestBody(exchange);
                // Simple JSON parsing
                String topicName = extractJsonValue(body, "topicName");
                int partitions = Integer.parseInt(extractJsonValue(body, "partitions"));
                int replicationFactor = Integer.parseInt(extractJsonValue(body, "replicationFactor"));
                
                boolean success = broker.createTopic(topicName, partitions, replicationFactor);
                String response = success 
                    ? "{\"success\":true,\"message\":\"Topic created\"}"
                    : "{\"success\":false,\"message\":\"Topic already exists\"}";
                sendResponse(exchange, success ? 200 : 400, response);
            } else {
                sendResponse(exchange, 405, "{\"error\":\"Method not allowed\"}");
            }
        }
    }
    
    private class ProduceHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if ("POST".equals(exchange.getRequestMethod())) {
                String body = readRequestBody(exchange);
                String topicName = extractJsonValue(body, "topicName");
                int partitionId = Integer.parseInt(extractJsonValue(body, "partitionId"));
                String message = extractJsonValue(body, "message");
                
                try {
                    long offset = broker.produce(topicName, partitionId, message.getBytes(StandardCharsets.UTF_8));
                    String response = "{\"success\":true,\"offset\":" + offset + "}";
                    sendResponse(exchange, 200, response);
                } catch (Exception e) {
                    String response = "{\"success\":false,\"error\":\"" + e.getMessage() + "\"}";
                    sendResponse(exchange, 400, response);
                }
            } else {
                sendResponse(exchange, 405, "{\"error\":\"Method not allowed\"}");
            }
        }
    }
    
    private class FetchHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if ("GET".equals(exchange.getRequestMethod())) {
                String query = exchange.getRequestURI().getQuery();
                String topicName = getQueryParam(query, "topicName");
                int partitionId = Integer.parseInt(getQueryParam(query, "partitionId"));
                long offset = Long.parseLong(getQueryParam(query, "offset"));
                int maxMessages = Integer.parseInt(getQueryParam(query, "maxMessages"));
                
                try {
                    List<LogSegment.MessageRecord> messages = broker.fetch(topicName, partitionId, offset, maxMessages);
                    String messagesJson = messages.stream()
                        .map(m -> "{\"offset\":" + m.getOffset() + ",\"message\":\"" + 
                            new String(m.getMessage(), StandardCharsets.UTF_8).replace("\"", "\\\"") + "\"}")
                        .collect(Collectors.joining(",", "[", "]"));
                    String response = "{\"success\":true,\"messages\":" + messagesJson + "}";
                    sendResponse(exchange, 200, response);
                } catch (Exception e) {
                    String response = "{\"success\":false,\"error\":\"" + e.getMessage() + "\"}";
                    sendResponse(exchange, 400, response);
                }
            } else {
                sendResponse(exchange, 405, "{\"error\":\"Method not allowed\"}");
            }
        }
    }
    
    private class BrokerStatusHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if ("GET".equals(exchange.getRequestMethod())) {
                String response = "{\"running\":" + broker.isRunning() + 
                    ",\"brokerId\":" + broker.getConfig().getBrokerId() + 
                    ",\"port\":" + broker.getConfig().getBrokerPort() + "}";
                sendResponse(exchange, 200, response);
            } else {
                sendResponse(exchange, 405, "{\"error\":\"Method not allowed\"}");
            }
        }
    }
    
    private class StaticFileHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            String path = exchange.getRequestURI().getPath();
            if (path.equals("/") || path.equals("/index.html")) {
                path = "/index.html";
            }
            
            try {
                java.io.InputStream is = RestApiServer.class.getResourceAsStream("/web" + path);
                if (is == null) {
                    sendResponse(exchange, 404, "{\"error\":\"Not found\"}");
                    return;
                }
                
                byte[] content = is.readAllBytes();
                String contentType = path.endsWith(".css") ? "text/css" : 
                                    path.endsWith(".js") ? "application/javascript" : "text/html";
                
                exchange.getResponseHeaders().set("Content-Type", contentType);
                exchange.sendResponseHeaders(200, content.length);
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(content);
                }
            } catch (Exception e) {
                sendResponse(exchange, 500, "{\"error\":\"" + e.getMessage() + "\"}");
            }
        }
    }
    
    private String extractJsonValue(String json, String key) {
        String pattern = "\"" + key + "\"\\s*:\\s*\"([^\"]+)\"";
        java.util.regex.Pattern p = java.util.regex.Pattern.compile(pattern);
        java.util.regex.Matcher m = p.matcher(json);
        if (m.find()) {
            return m.group(1);
        }
        pattern = "\"" + key + "\"\\s*:\\s*(\\d+)";
        p = java.util.regex.Pattern.compile(pattern);
        m = p.matcher(json);
        if (m.find()) {
            return m.group(1);
        }
        return "";
    }
    
    private String getQueryParam(String query, String param) {
        if (query == null) return "";
        String[] pairs = query.split("&");
        for (String pair : pairs) {
            String[] keyValue = pair.split("=");
            if (keyValue.length == 2 && keyValue[0].equals(param)) {
                return keyValue[1];
            }
        }
        return "";
    }
    
    public static void main(String[] args) throws Exception {
        BrokerConfig config = ConfigLoader.loadConfig();
        Broker broker = new Broker(config);
        
        // Start broker
        new Thread(() -> {
            try {
                broker.start();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }).start();
        
        // Wait for broker to start
        Thread.sleep(2000);
        
        // Start REST API server
        RestApiServer apiServer = new RestApiServer(broker, 8080);
        apiServer.start();
        
        System.out.println("Broker running on port " + config.getBrokerPort());
        System.out.println("REST API running on http://localhost:8080");
        System.out.println("Frontend available at http://localhost:8080/index.html");
        
        // Keep running
        Thread.currentThread().join();
    }
}

