package com.asyncloadtest.core.models;

import lombok.Value;

@Value
public class Subscription {
    String userId;
    String channelId;
    long timestamp;
}