package com.asyncloadtest.core.websocket;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import io.vertx.core.json.JsonArray;
import io.vertx.ext.web.Router;
import io.vertx.core.http.ServerWebSocket;
import io.vertx.ext.web.handler.StaticHandler;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.apache.commons.codec.digest.DigestUtils;

public class WebSocketVerticle extends AbstractVerticle {
    private static final Logger log = LoggerFactory.getLogger(WebSocketVerticle.class);
    public final AmazonDynamoDB dynamoDB;
    public WebSocketVerticle(AmazonDynamoDB dynamoDB) {
        this.dynamoDB = dynamoDB;
    }
    private static final String CONNECTIONS_TABLE = "Connections";
    private static final String MESSAGES_TABLE = "Messages";

    @Override
    public void start(Promise<Void> startPromise) {
        Router router = Router.router(vertx);

        // Add static handler for serving the test page
        log.info("Setting up static file handler for webroot directory");
        StaticHandler staticHandler = StaticHandler.create()
                .setWebRoot("webroot")
                .setCachingEnabled(false)
                .setDirectoryListing(true);
        router.route("/*").handler(staticHandler);

        // Setup WebSocket handling
        vertx.createHttpServer()
                .webSocketHandler(this::handleWebSocket)
                .requestHandler(router)
                .listen(8080, result -> {
                    if (result.succeeded()) {
                        log.info("WebSocket server started on port 8080");
                        startPromise.complete();
                    } else {
                        log.error("Failed to start server", result.cause());
                        startPromise.fail(result.cause());
                    }
                });
    }


    private final ConcurrentHashMap<ServerWebSocket, String> connectionIds = new ConcurrentHashMap<>();
    /**
     * Handles new WebSocket connections
     * @param websocket The ServerWebSocket instance for the new connection
     */
    private void handleWebSocket(ServerWebSocket websocket) {
        // Generate and store connection ID
        String connectionId = UUID.randomUUID().toString();
        connectionIds.put(websocket, connectionId);

        log.info("New WebSocket connection with ID: {}", connectionId);
        websocket.accept();

        websocket.handler(buffer -> {
            JsonObject message = new JsonObject(buffer.toString());
            if ("handshake".equals(message.getString("type"))) {
                String storedConnectionId = connectionIds.get(websocket);
                handleHandshake(websocket, message, storedConnectionId)
                        .compose(v -> loadPreviousState(message.getInteger("clientId")))
                        .compose(state -> sendInitialState(websocket, state))
                        .onSuccess(v -> startMessageLoop(websocket))
                        .onFailure(err -> handleError(websocket, err));
            }
        });

        // Don't forget to clean up
        websocket.closeHandler(v -> {
            connectionIds.remove(websocket);
            handleDisconnect(websocket);
        });
    }

    /**
     * Processes the initial handshake message from a client
     * @param websocket The ServerWebSocket instance
     * @param handshake The handshake message as JsonObject
     * @return Future<Void> completing when handshake is processed
     */
    private Future<Void> handleHandshake(ServerWebSocket websocket, JsonObject handshake, String connectionId) {
        return Future.future(promise -> {
            // Debug
            log.info("Before setting header");
            websocket.headers().set("connectionId", UUID.randomUUID().toString());
            log.info("Headers after setting: {}", websocket.headers().entries());

            //

            // Validate handshake message
            Integer clientId = handshake.getInteger("clientId");
            if (clientId == null) {
                promise.fail("Invalid handshake: missing clientId");
                return;
            }

            // Validate connectionId
            if (connectionId == null || connectionId.isEmpty()) {
                promise.fail("Invalid connection ID: null or empty");
                return;
            }

            // Store connection in DynamoDB with guaranteed unique connectionId
            Map<String, AttributeValue> item = new HashMap<>();
            item.put("ConnectionId", new AttributeValue(connectionId));
            item.put("ClientId", new AttributeValue().withN(clientId.toString()));
            item.put("LastSeen", new AttributeValue().withN(String.valueOf(System.currentTimeMillis())));
            item.put("Instance", new AttributeValue(getInstanceId()));

            PutItemRequest putRequest = new PutItemRequest()
                    .withTableName(CONNECTIONS_TABLE)
                    .withItem(item);

            try {
                dynamoDB.putItem(putRequest);
                log.info("Successfully stored connection {} for client {}", connectionId, clientId);
                promise.complete();
            } catch (Exception e) {
                log.error("DynamoDB operation failed for clientId={}: {}",
                        clientId,
                        e.getMessage(),
                        e
                );
                promise.fail(e);
            }
        });
    }

    private Future<JsonObject> loadPreviousState(Integer clientId) {
        return Future.future(promise -> {
            // Query DynamoDB for previous messages
            QueryRequest queryRequest = new QueryRequest()
                    .withTableName(MESSAGES_TABLE)
                    .withKeyConditionExpression("ClientId = :clientId")
                    .withExpressionAttributeValues(new HashMap<String, AttributeValue>() {{
                        put(":clientId", new AttributeValue().withN(clientId.toString()));
                    }});

            try {
                QueryResult result = dynamoDB.query(queryRequest);
                // Convert result to JsonObject containing previous state
                JsonObject state = new JsonObject();
                // ... process results ...
                promise.complete(state);
            } catch (Exception e) {
                promise.fail(e);
            }
        });
    }

    private Future<Void> sendInitialState(ServerWebSocket websocket, JsonObject state) {
        return Future.future(promise -> {
            websocket.writeTextMessage(state.encode());
            promise.complete();
        });
    }

    private void startMessageLoop(ServerWebSocket websocket) {
        websocket.handler(buffer -> {
            JsonObject message = new JsonObject(buffer.toString());
            handleMessage(websocket, message)
                    .onFailure(err -> handleError(websocket, err));
        });
    }

    // Window for state synchronization - 15 seconds
    // TODO: Consider adding buffer time for processing latency. Messages arriving
    // near the cutoff time might be rejected due to processing delays, even if
    // they were sent within the valid window.
    private static final long STATE_WINDOW_MS = 15000;
    private Map<Long, String> recentChecksums = new ConcurrentHashMap<>();
    private Map<Long, List<JsonObject>> recentMessages = new ConcurrentHashMap<>();

    private Future<Void> handleMessage(ServerWebSocket websocket, JsonObject message) {
        return Future.future(promise -> {
            long timestamp = message.getLong("timestamp");
            String incomingChecksum = message.getString("checksum");

            // First, cleanup old messages
            long cutoffTime = System.currentTimeMillis() - STATE_WINDOW_MS;
            recentMessages.entrySet().removeIf(entry -> entry.getKey() < cutoffTime);
            recentChecksums.entrySet().removeIf(entry -> entry.getKey() < cutoffTime);

            // Verify timestamp is within window
            if (timestamp < cutoffTime) {
                log.warn("Message rejected - outside time window. ClientId={}, messageTimestamp={}, cutoffTime={}",
                        message.getInteger("clientId"),
                        timestamp,
                        cutoffTime
                );
                websocket.writeTextMessage(new JsonObject()
                        .put("type", "error")
                        .put("code", "SYNC_ERROR")
                        .put("message", "Client too far behind")
                        .encode());
                websocket.close();
                promise.complete();
                return;
            }

            // Verify checksum if we have one for this timestamp
            String expectedChecksum = recentChecksums.get(timestamp);
            if (expectedChecksum != null && !expectedChecksum.equals(incomingChecksum)) {
                log.error("Checksum mismatch detected for clientId={} at timestamp={}. Expected: {}, Received: {}",
                        message.getInteger("clientId"),
                        timestamp,
                        expectedChecksum,
                        incomingChecksum
                );

                websocket.writeTextMessage(new JsonObject()
                        .put("type", "error")
                        .put("code", "CHECKSUM_MISMATCH")
                        .put("message", "State mismatch detected")
                        .encode());
                websocket.close();
                promise.complete();
                return;
            }

            // Store message
            recentMessages.computeIfAbsent(timestamp, k -> new ArrayList<>())
                    .add(message);

            // Calculate new checksum
            String newChecksum = calculateChecksum(timestamp);
            recentChecksums.put(timestamp, newChecksum);

            // Store in DynamoDB for recovery
            storeToDynamo(message, newChecksum)
                    .onSuccess(v -> {
                        // Broadcast to all clients
                        JsonObject update = new JsonObject()
                                .put("type", "update")
                                .put("entities", new JsonArray(recentMessages.get(timestamp)))
                                .put("checksum", newChecksum)
                                .put("timestamp", timestamp);

                        vertx.eventBus().publish("updates", update);
                        promise.complete();
                    })
                    .onFailure(promise::fail);
        });
    }

    private String calculateChecksum(long timestamp) {
        // Get messages for this timestamp
        List<JsonObject> messages = recentMessages.get(timestamp);
        if (messages == null) return "";

        // Sort to ensure consistent ordering
        messages.sort((a, b) -> a.getInteger("clientId").compareTo(b.getInteger("clientId")));

        // Create checksum
        String concatenated = messages.stream()
                .map(JsonObject::encode)
                .collect(Collectors.joining());

        return DigestUtils.sha256Hex(concatenated);
    }

    private Future<Void> storeToDynamo(JsonObject message, String checksum) {
        return Future.future(promise -> {
            PutItemRequest putRequest = new PutItemRequest()
                    .withTableName(MESSAGES_TABLE)
                    .withItem(new HashMap<String, AttributeValue>() {{
                        put("ClientId", new AttributeValue().withN(message.getInteger("clientId").toString()));
                        put("Timestamp", new AttributeValue().withN(message.getLong("timestamp").toString()));
                        put("Message", new AttributeValue(message.getString("message")));
                        put("Checksum", new AttributeValue(checksum));
                    }});

            try {
                dynamoDB.putItem(putRequest);
                promise.complete();
            } catch (Exception e) {
                promise.fail(e);
            }
        });
    }

    /**
     * Handles disconnection of a WebSocket client
     * @param websocket The ServerWebSocket instance being disconnected
     */
    private void handleDisconnect(ServerWebSocket websocket) {
        log.info("WebSocket disconnection from {}", websocket.remoteAddress());

        // Remove connection from DynamoDB
        Map<String, AttributeValue> key = new HashMap<>();
        key.put("ConnectionId", new AttributeValue(websocket.textHandlerID()));

        DeleteItemRequest deleteRequest = new DeleteItemRequest()
                .withTableName(CONNECTIONS_TABLE)
                .withKey(key);

        try {
            dynamoDB.deleteItem(deleteRequest);
            log.debug("Successfully removed connection {} from DynamoDB", websocket.textHandlerID());
        } catch (Exception e) {
            log.error("Failed to remove connection {} from DynamoDB: {}",
                    websocket.textHandlerID(),
                    e.getMessage(),
                    e
            );
        }
    }

    private void handleError(ServerWebSocket websocket, Throwable error) {
        JsonObject errorMessage = new JsonObject()
                .put("type", "error")
                .put("message", error.getMessage());
        websocket.writeTextMessage(errorMessage.encode());
    }

    private String getInstanceId() {
        // Return unique identifier for this server instance
        String instanceId = System.getenv("INSTANCE_ID");
        return instanceId != null ? instanceId : "local-instance";
    }
}