// core/state/ChecksumManager.java
package com.asyncloadtest.core.state;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.*;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import io.vertx.core.json.JsonObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.digest.DigestUtils;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.HashMap;
import java.util.Map;

@Slf4j
@Singleton
public class ChecksumManager {
    private static final String CHECKSUMS_TABLE = "Checksums";
    private static final long CHECKSUM_TTL_SECONDS = 12;

    private final AmazonDynamoDB dynamoDB;
    private final IMap<String, Map<Long, String>> checksumCache;

    @Inject
    public ChecksumManager(AmazonDynamoDB dynamoDB, HazelcastInstance hazelcast) {
        this.dynamoDB = dynamoDB;
        this.checksumCache = hazelcast.getMap("checksums");
    }

    public String generateChecksum(String channelId, JsonObject state) {
        String checksum = DigestUtils.sha256Hex(state.encode());
        long timestamp = System.currentTimeMillis();
        long expiryTime = timestamp + (CHECKSUM_TTL_SECONDS * 1000);

        // Store in DynamoDB
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("Channel", new AttributeValue(channelId));
        item.put("Timestamp", new AttributeValue().withN(String.valueOf(timestamp)));
        item.put("Checksum", new AttributeValue(checksum));
        item.put("ExpiryTime", new AttributeValue().withN(String.valueOf(expiryTime)));

        try {
            dynamoDB.putItem(new PutItemRequest()
                    .withTableName(CHECKSUMS_TABLE)
                    .withItem(item));
        } catch (Exception e) {
            log.error("Failed to store checksum in DynamoDB", e);
            // Continue anyway as we have it in Hazelcast
        }

        // Store in Hazelcast
        Map<Long, String> channelChecksums = checksumCache.getOrDefault(channelId, new HashMap<>());
        channelChecksums.put(timestamp, checksum);
        checksumCache.put(channelId, channelChecksums);

        return checksum;
    }

    public boolean validateChecksum(String channelId, long timestamp, String checksum) {
        // Try Hazelcast first
        Map<Long, String> channelChecksums = checksumCache.get(channelId);
        if (channelChecksums != null) {
            String cachedChecksum = channelChecksums.get(timestamp);
            if (cachedChecksum != null) {
                return checksum.equals(cachedChecksum);
            }
        }

        // Fall back to DynamoDB
        try {
            Map<String, AttributeValue> key = new HashMap<>();
            key.put("Channel", new AttributeValue(channelId));
            key.put("Timestamp", new AttributeValue().withN(String.valueOf(timestamp)));

            GetItemResult result = dynamoDB.getItem(new GetItemRequest()
                    .withTableName(CHECKSUMS_TABLE)
                    .withKey(key));

            if (result.getItem() != null) {
                String storedChecksum = result.getItem().get("Checksum").getS();
                return checksum.equals(storedChecksum);
            }
        } catch (Exception e) {
            log.error("Failed to retrieve checksum from DynamoDB", e);
        }

        return false;
    }

    public void cleanupOldChecksums() {
        long cutoffTime = System.currentTimeMillis() - (CHECKSUM_TTL_SECONDS * 1000);

        // Clean Hazelcast
        for (Map.Entry<String, Map<Long, String>> entry : checksumCache.entrySet()) {
            Map<Long, String> channelChecksums = entry.getValue();
            channelChecksums.entrySet().removeIf(e -> e.getKey() < cutoffTime);
            if (channelChecksums.isEmpty()) {
                checksumCache.remove(entry.getKey());
            } else {
                checksumCache.put(entry.getKey(), channelChecksums);
            }
        }

        // DynamoDB cleanup handled by TTL
    }
}