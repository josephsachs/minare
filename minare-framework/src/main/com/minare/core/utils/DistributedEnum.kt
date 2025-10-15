import com.google.inject.Inject
import com.google.inject.Singleton
import com.hazelcast.core.HazelcastInstance
import com.hazelcast.cp.IAtomicReference
import kotlin.reflect.KClass

class DistributedEnum<T : Enum<T>>(
    hazelcastInstance: HazelcastInstance,
    name: String,
    private val enumClass: KClass<T>,
    private val initialValue: T
) {
    @Singleton
    class Factory @Inject constructor(
        private val hazelcastInstance: HazelcastInstance
    ) {
        fun <T : Enum<T>> create(name: String,
                                 enumClass: KClass<T>,
                                 initialValue: T
        ): DistributedEnum<T> {
            return DistributedEnum(
                hazelcastInstance,
                name,
                enumClass,
                initialValue
            )
        }
    }

    private val atomicRef: IAtomicReference<String> = hazelcastInstance.cpSubsystem
        .getAtomicReference(name)

    init {
        atomicRef.compareAndSet(null, initialValue.name)
    }

    fun get(): T = atomicRef.get()?.let { storedName ->
        enumClass.java.enumConstants.first { it.name == storedName }
    } ?: initialValue

    fun set(value: T) {
        atomicRef.set(value.name)
    }

    fun getAndSet(value: T): T? =
        atomicRef.getAndSet(value.name)?.let { storedName ->
            enumClass.java.enumConstants.first { it.name == storedName }
        }

    fun compareAndSet(expected: T, update: T): Boolean =
        atomicRef.compareAndSet(expected.name, update.name)
}