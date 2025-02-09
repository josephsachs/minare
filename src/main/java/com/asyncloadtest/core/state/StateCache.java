// core/state/StateCache.java
package com.asyncloadtest.core.state;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import io.vertx.core.json.JsonObject;
import org.apache.commons.codec.digest.DigestUtils;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;

@Singleton
public class StateCache {
    private static final long CHECKSUM_TTL_SECONDS = 12;

    private final IMap<String, JsonObject> currentState;
    private final IMap<String, Map<Long, String>> checksumsByChannel;

    @Inject
    public StateCache(HazelcastInstance hazelcast) {
        this.currentState = hazelcast.getMap("entityState");
        this.checksumsByChannel = hazelcast.getMap("checksums");
    }

    public String generateAndStoreChecksum(String channelId, JsonObject state, long timestamp) {
        String checksum = DigestUtils.sha256Hex(state.encode());

        Map<Long, String> channelChecksums = checksumsByChannel.getOrDefault(channelId, new HashMap<>());
        channelChecksums.put(timestamp, checksum);
        checksumsByChannel.put(channelId, channelChecksums, CHECKSUM_TTL_SECONDS, TimeUnit.SECONDS);

        return checksum;
    }

    public boolean validateChecksum(String channelId, long timestamp, String checksum) {
        Map<Long, String> channelChecksums = checksumsByChannel.get(channelId);
        if (channelChecksums == null) return false;

        String expectedChecksum = channelChecksums.get(timestamp);
        return checksum.equals(expectedChecksum);
    }
}