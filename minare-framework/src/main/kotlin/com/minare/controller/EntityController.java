// controllers/AbstractEntityController.java
package kotlin.com.minare.controller;

import com.minare.persistence.EntityStore;
import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;

import javax.inject.Inject;

public abstract class EntityController {
    private final EntityStore entityStore;
    protected abstract boolean hasAccess(String connectionId, String entityId);
    protected abstract boolean validateUpdate(JsonObject currentState, JsonObject proposedUpdate);

    @Inject
    public EntityController(EntityStore entityStore) {
        this.entityStore = entityStore;
    }

    /**public void handleUpdate(String entityId, JsonObject update, long version) {
        entityStore.find(entityId)
                .compose(current -> {
                    if (current == null) {
                        return Future.failedFuture(new IllegalStateException("entity not found"));
                    }

                    if (!validateUpdate(JsonObject.mapFrom(current), update)) {
                        return Future.failedFuture(new IllegalStateException("Invalid update"));
                    }

                    return entityStore.update(entityId, update)
                            .recover(e -> Future.failedFuture(new IllegalStateException("entity was modified by another request")));
                })
                .onSuccess(v -> System.out.println("Update successful"))
                .onFailure(Throwable::printStackTrace);
    }**/
}