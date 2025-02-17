package com.minare.persistence;

import com.minare.core.models.User;
import io.vertx.core.Future;

import java.util.Set;

public interface UserStore {
    Future<User> update(String userId, String connectionId);
    Future<User> find(String userId);
    Future<Set<User>> findAll(Set<String> userIds);
}