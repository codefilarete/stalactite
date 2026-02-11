package org.codefilarete.stalactite.dsl.idpolicy;

import java.sql.PreparedStatement;
import java.util.function.Consumer;
import java.util.function.Function;

import org.codefilarete.stalactite.mapping.id.manager.IdentifierInsertionManager;
import org.codefilarete.stalactite.mapping.id.sequence.DatabaseSequenceSettings;
import org.codefilarete.stalactite.mapping.id.sequence.hilo.PooledHiLoSequence;
import org.codefilarete.stalactite.mapping.id.sequence.hilo.PooledHiLoSequenceStorageOptions;
import org.codefilarete.tool.function.Sequence;

/**
 * Available identifier policies for entities.
 *
 * @see IdentifierInsertionManager
 */
@SuppressWarnings("java:S2326" /* Unused generics is necessary to caller signature (mapKey) to make policy identifier type match entity identifier one */)
public interface IdentifierPolicy<ID> {
	/**
	 * Policy for entities that have their id given by the database after insert, such as increment column, implying that generated values can be
	 * read through {@link PreparedStatement#getGeneratedKeys()}
	 *
	 * @return a new policy that will be used to get the identifier value
	 */
	static <I> GeneratedKeysPolicy<I> databaseAutoIncrement() {
		return new GeneratedKeysPolicySupport<>();
	}
	
	/**
	 * Policy for entities that want their id fixed just before insert which value is given by a {@link Sequence}.
	 * Reader may be interested in {@link PooledHiLoSequence}.
	 * Sequence data will be stored as specified through {@link PooledHiLoSequenceStorageOptions#DEFAULT}
	 *
	 * @return a new policy that will be used to get the identifier value
	 * @see #pooledHiLoSequence(PooledHiLoSequenceStorageOptions)
	 */
	static BeforeInsertIdentifierPolicy<Long> pooledHiLoSequence() {
		return new PooledHiLoSequenceIdentifierPolicySupport();
	}
	
	/**
	 * Policy for entities that want their id fixed just before insert which value is given by a {@link Sequence}.
	 * Reader may be interested in {@link PooledHiLoSequence}.
	 *
	 * @param sequenceStorageOptions the options about table to store sequence data
	 * @return a new policy that will be used to get the identifier value
	 * @see PooledHiLoSequenceStorageOptions#DEFAULT
	 * @see PooledHiLoSequenceStorageOptions#HIBERNATE_DEFAULT
	 */
	static BeforeInsertIdentifierPolicy<Long> pooledHiLoSequence(PooledHiLoSequenceStorageOptions sequenceStorageOptions) {
		return new PooledHiLoSequenceIdentifierPolicySupport(sequenceStorageOptions);
	}
	
	static BeforeInsertIdentifierPolicy<Long> databaseSequence(DatabaseSequenceNamingStrategy namingStrategy) {
		return new DatabaseSequenceIdentifierPolicySupport(namingStrategy);
	}
	
	static BeforeInsertIdentifierPolicy<Long> databaseSequence(String name) {
		return new DatabaseSequenceIdentifierPolicySupport(entityType -> name);
	}
	
	static BeforeInsertIdentifierPolicy<Long> databaseSequence(DatabaseSequenceNamingStrategy namingStrategy, DatabaseSequenceSettings databaseSequenceSettings) {
		return new DatabaseSequenceIdentifierPolicySupport(namingStrategy, databaseSequenceSettings);
	}
	
	static BeforeInsertIdentifierPolicy<Long> databaseSequence(String name, DatabaseSequenceSettings databaseSequenceSettings) {
		return new DatabaseSequenceIdentifierPolicySupport(entityType -> name, databaseSequenceSettings);
	}
	
	/**
	 * Policy for entities that want their id fixed just before insert which value is given by a {@link Sequence}.
	 * Be aware that the given sequence will be shared across all managed entities, meaning that, for instance,
	 * if it is a long sequence, an integer value can't be found twice in whole entities.
	 * Reader may be interested in other {@link #pooledHiLoSequence()} methods to avoid such sharing behavior.
	 *
	 * @param sequence the {@link Sequence} to ask for identifier value
	 * @param <I> identifier type
	 * @return a new policy that will be used to get the identifier value
	 */
	static <I> BeforeInsertIdentifierPolicy<I> pooledHiLoSequence(Sequence<I> sequence) {
		return new BeforeInsertIdentifierPolicySupport<>(sequence);
	}
	
	/**
	 * Policy for entities that have their id already fixed and doesn't require the persistence engine to generate an identifier for them.
	 * Meanwhile, this policy has a trade-off on persist(..) operation: without knowing how to determine if
	 * entities are new or not, a database round-trip must be performed to get the ones already present in the database
	 * and then choose to update them, not insert them. This round-trip may impact performances. That's why it is
	 * preferable to use {@link #alreadyAssigned(Consumer, Function)} to provide a way to get the state of the entities.
	 * 
	 * @param <C> entity type
	 * @param <I> identifier type
	 * @return a new policy that will be used to know the persistent state of entities
	 */
	static <C, I> AlreadyAssignedIdentifierPolicy<C, I> alreadyAssigned() {
		return IdentifierPolicy.alreadyAssigned(null, null);
	}
	
	/**
	 * Policy for entities that have their id already fixed and doesn't require the persistence engine to generate an identifier for them.
	 * Meanwhile, this policy requires those instances to be capable of being marked as persisted (after insert to prevent the engine from
	 * trying to persist again an already persisted instance, for example). A basic implementation can be a boolean switch on entity.
	 *
	 * @param markAsPersistedFunction the {@link Consumer} that allows marking the entity as "inserted in database"
	 * @param isPersistedFunction the {@link Function} that allows knowing if entity was already inserted in database
	 * @param <C> entity type
	 * @param <I> identifier type
	 * @return a new policy that will be used to know the persistent state of entities
	 */
	static <C, I> AlreadyAssignedIdentifierPolicy<C, I> alreadyAssigned(Consumer<C> markAsPersistedFunction,
																		Function<C, Boolean> isPersistedFunction) {
		return new AlreadyAssignedIdentifierPolicySupport<>(markAsPersistedFunction, isPersistedFunction);
	}
}
