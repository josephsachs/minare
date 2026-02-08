import com.google.inject.Provider
import com.minare.application.interfaces.AppState
import com.minare.controller.ChannelController
import com.minare.controller.EntityController
import com.minare.core.entity.models.Entity
import javax.inject.Inject
import javax.inject.Singleton

/**
 * Example-specific extension of the framework's EntityController.
 */
@Singleton
class NodeGraphEntityController @Inject constructor(
    private val appStateProvider: Provider<AppState>
) : EntityController() {

    override suspend fun setId(entity: Entity) {
        val withoutSuffix = entity._id.removeSuffix("-unsaved")
        entity._id = withoutSuffix
    }
}