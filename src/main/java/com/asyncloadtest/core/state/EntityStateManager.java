// core/state/EntityStateManager.java
package com.asyncloadtest.core.state;

import com.asyncloadtest.core.models.StateUpdate;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.subjects.BehaviorSubject;
import io.vertx.core.json.JsonObject;
import lombok.extern.slf4j.Slf4j;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Singleton
public class EntityStateManager {
    private final StateCache stateCache;
    private final Map<String, BehaviorSubject<StateUpdate>> channelSubjects = new ConcurrentHashMap<>();

    @Inject
    public EntityStateManager(StateCache stateCache) {
        this.stateCache = stateCache;
    }

    public Observable<StateUpdate> subscribeToChannel(String channelId) {
        return channelSubjects.computeIfAbsent(channelId, k -> BehaviorSubject.create())
                .distinctUntilChanged(update -> update.getState().encode());
    }

    public void updateState(String channelId, JsonObject newState) {
        long timestamp = System.currentTimeMillis();
        String checksum = stateCache.generateAndStoreChecksum(channelId, newState, timestamp);

        StateUpdate update = new StateUpdate(channelId, timestamp, newState, checksum);
        BehaviorSubject<StateUpdate> subject = channelSubjects.get(channelId);
        if (subject != null) {
            subject.onNext(update);
        }
    }
}