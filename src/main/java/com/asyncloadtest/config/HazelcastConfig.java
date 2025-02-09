// config/HazelcastConfig.java
package com.asyncloadtest.config;

import com.hazelcast.config.*;
import lombok.extern.slf4j.Slf4j;

import javax.inject.Singleton;
import java.util.concurrent.TimeUnit;

@Slf4j
@Singleton
public class HazelcastConfig {

    public Config getConfig() {
        Config config = new Config();
        config.setClusterName("asyncloadtest");

        // Configure map for checksums with 12-second TTL
        MapConfig checksumsConfig = new MapConfig("checksums")
                .setTimeToLiveSeconds(12)
                .setEvictionConfig(
                        new EvictionConfig()
                                .setEvictionPolicy(EvictionPolicy.LRU)
                                .setMaxSizePolicy(MaxSizePolicy.PER_NODE)
                                .setSize(10000)
                );

        // Configure map for entity state
        MapConfig stateConfig = new MapConfig("entityState")
                .setEvictionConfig(
                        new EvictionConfig()
                                .setEvictionPolicy(EvictionPolicy.LRU)
                                .setMaxSizePolicy(MaxSizePolicy.PER_NODE)
                                .setSize(10000)
                );

        config.addMapConfig(checksumsConfig);
        config.addMapConfig(stateConfig);

        return config;
    }
}