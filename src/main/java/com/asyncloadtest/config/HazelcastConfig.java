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

        // Disable unnecessary features
        config.getJetConfig().setEnabled(false);
        config.getCPSubsystemConfig().setCPMemberCount(0);
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        config.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true)
                .addMember("127.0.0.1");

        // Configure maps we actually need
        MapConfig checksumsConfig = new MapConfig("checksums")
                .setTimeToLiveSeconds(12)
                .setEvictionConfig(
                        new EvictionConfig()
                                .setEvictionPolicy(EvictionPolicy.LRU)
                                .setMaxSizePolicy(MaxSizePolicy.PER_NODE)
                                .setSize(10000)
                );

        config.addMapConfig(checksumsConfig);

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