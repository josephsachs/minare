package com.asyncloadtest.persistence;

import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.dynamodbv2.model.*;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import lombok.extern.slf4j.Slf4j;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.Arrays;
import java.util.List;

@Slf4j
@Singleton
public class DynamoDBManager {
    private final AmazonDynamoDB dynamoDB;
    private static final String SUBSCRIPTIONS_TABLE = "Subscriptions";
    private static final String CHECKSUMS_TABLE = "Checksums";
    private final Vertx vertx;

    @Inject
    public DynamoDBManager(boolean isLocal, AmazonDynamoDB dynamoDB, Vertx vertx) {
        this.vertx = vertx;
        initializeTables();

        if (isLocal) {
            this.dynamoDB = AmazonDynamoDBClientBuilder.standard()
                    .withEndpointConfiguration(
                            new AwsClientBuilder.EndpointConfiguration(
                                    "http://localhost:8000",
                                    "us-west-2"
                            )
                    )
                    .withCredentials(new AWSStaticCredentialsProvider(
                            new BasicAWSCredentials("dummy", "dummy")))
                    .build();
        } else {
            this.dynamoDB = AmazonDynamoDBClientBuilder.defaultClient();
        }
        createTablesIfNotExist();
    }

    private void initializeTables() {
        // Convert to non-blocking with Vert.x
        vertx.executeBlocking(promise -> {
            try {
                createTablesIfNotExist();
                promise.complete();
            } catch (Exception e) {
                promise.fail(e);
            }
        }, result -> {
            if (result.succeeded()) {
                log.info("DynamoDB tables initialized successfully");
            } else {
                log.error("Failed to initialize DynamoDB tables", result.cause());
            }
        });
    }

    private void createTablesIfNotExist() {
        log.info("Ensuring required tables exist...");
        try {
            createSubscriptionsTable();
            createChecksumsTable();
        } catch (Exception e) {
            log.error("Failed to create tables", e);
            throw e;
        }
    }

    private Future<Void> waitForTableDeletion(String tableName) {
        Promise<Void> promise = Promise.promise();

        // Use recursive pattern with Vert.x timer instead of blocking
        vertx.setPeriodic(1000, timerId -> {
            try {
                dynamoDB.describeTable(tableName);
            } catch (ResourceNotFoundException e) {
                vertx.cancelTimer(timerId);
                promise.complete();
            }
        });

        return promise.future();
    }

    private void createSubscriptionsTable() {
        boolean forceRecreate = "true".equalsIgnoreCase(System.getenv("TEST_DATABASE_SETUP"));

        if (tableExists(SUBSCRIPTIONS_TABLE)) {
            log.info("Subscriptions table exists");
            if (forceRecreate) {
                try {
                    log.info("TEST_DATABASE_SETUP=true, recreating Subscriptions table");
                    dynamoDB.deleteTable(SUBSCRIPTIONS_TABLE);
                    waitForTableDeletion(SUBSCRIPTIONS_TABLE).onComplete(ar -> {
                        if (ar.succeeded()) {
                            createNewSubscriptionsTable();
                        } else {
                            log.error("Failed during table recreation", ar.cause());
                        }
                    });
                } catch (Exception e) {
                    log.error("Failed to delete existing Subscriptions table", e);
                    throw e;
                }
            }
            return;
        }
        createNewSubscriptionsTable();
    }

    private void createNewSubscriptionsTable() {
        if (tableExists(SUBSCRIPTIONS_TABLE)) {
            log.info("Subscriptions table exists");
            return;
        }

        List<KeySchemaElement> keySchema = Arrays.asList(
                new KeySchemaElement("Channel", KeyType.HASH),
                new KeySchemaElement("Timestamp", KeyType.RANGE)
        );

        List<AttributeDefinition> attributeDefinitions = Arrays.asList(
                new AttributeDefinition("Channel", ScalarAttributeType.S),
                new AttributeDefinition("Timestamp", ScalarAttributeType.N),
                // Only include UserId in attribute definitions since it's used in GSI
                new AttributeDefinition("UserId", ScalarAttributeType.S)
        );

        GlobalSecondaryIndex userIndex = new GlobalSecondaryIndex()
                .withIndexName("UserSubscriptions")
                .withKeySchema(Arrays.asList(
                        new KeySchemaElement("UserId", KeyType.HASH),
                        new KeySchemaElement("Timestamp", KeyType.RANGE)
                ))
                .withProjection(new Projection().withProjectionType(ProjectionType.ALL))
                .withProvisionedThroughput(new ProvisionedThroughput(5L, 5L));

        CreateTableRequest request = new CreateTableRequest()
                .withTableName(SUBSCRIPTIONS_TABLE)
                .withKeySchema(keySchema)
                .withAttributeDefinitions(attributeDefinitions)
                .withGlobalSecondaryIndexes(userIndex)
                .withProvisionedThroughput(new ProvisionedThroughput(5L, 5L));

        try {
            dynamoDB.createTable(request);
            log.info("Created Subscriptions table");
        } catch (Exception e) {
            log.error("Failed to create Subscriptions table", e);
            throw e;
        }
    }

    private void createChecksumsTable() {
        if (tableExists(CHECKSUMS_TABLE)) {
            log.info("Checksums table exists");
            return;
        }

        List<KeySchemaElement> keySchema = Arrays.asList(
                new KeySchemaElement("Channel", KeyType.HASH),
                new KeySchemaElement("Timestamp", KeyType.RANGE)
        );

        List<AttributeDefinition> attributeDefinitions = Arrays.asList(
                new AttributeDefinition("Channel", ScalarAttributeType.S),
                new AttributeDefinition("Timestamp", ScalarAttributeType.N)
        );

        CreateTableRequest request = new CreateTableRequest()
                .withTableName(CHECKSUMS_TABLE)
                .withKeySchema(keySchema)
                .withAttributeDefinitions(attributeDefinitions)
                .withProvisionedThroughput(new ProvisionedThroughput(5L, 5L));

        dynamoDB.createTable(request);

        // Enable TTL after table creation
        UpdateTimeToLiveRequest ttlRequest = new UpdateTimeToLiveRequest()
                .withTableName(CHECKSUMS_TABLE)
                .withTimeToLiveSpecification(new TimeToLiveSpecification()
                        .withAttributeName("ExpiryTime")
                        .withEnabled(true));

        dynamoDB.updateTimeToLive(ttlRequest);

        log.info("Created Checksums table with TTL enabled");
    }

    private boolean tableExists(String tableName) {
        try {
            dynamoDB.describeTable(tableName);
            return true;
        } catch (ResourceNotFoundException e) {
            return false;
        }
    }

    public AmazonDynamoDB getClient() {
        return dynamoDB;
    }
}