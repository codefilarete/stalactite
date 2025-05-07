package org.codefilarete.stalactite.engine;

import java.sql.PreparedStatement;
import java.util.function.Consumer;
import java.util.function.Function;

import org.codefilarete.stalactite.mapping.id.manager.IdentifierInsertionManager;
import org.codefilarete.stalactite.mapping.id.sequence.DatabaseSequenceSettings;
import org.codefilarete.stalactite.mapping.id.sequence.hilo.PooledHiLoSequence;
import org.codefilarete.stalactite.mapping.id.sequence.hilo.PooledHiLoSequenceStorageOptions;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.statement.binder.ParameterBinder;
import org.codefilarete.tool.Strings;
import org.codefilarete.tool.function.Converter;
import org.codefilarete.tool.function.Sequence;

/**
 * @author Guillaume Mary
 */
public interface ColumnOptions<O> extends PropertyOptions<O> {
	
	/**
	 * Marks the property as mandatory, which makes the mapped column not nullable: does not make a null checking at runtime.
	 * Note that using this method on an identifier one as no purpose because identifiers are already mandatory.
	 */
	@Override
	ColumnOptions<O> mandatory();
	
	@Override
	ColumnOptions<O> setByConstructor();
	
	@Override
	ColumnOptions<O> readonly();
	
	@Override
	ColumnOptions<O> columnName(String name);
	
	@Override
	ColumnOptions<O> column(Column<? extends Table, ? extends O> column);
	
	@Override
	ColumnOptions<O> fieldName(String name);
	
	@Override
	ColumnOptions<O> readConverter(Converter<O, O> converter);
	
	@Override
	ColumnOptions<O> writeConverter(Converter<O, O> converter);
	
	@Override
	<V> PropertyOptions<O> sqlBinder(ParameterBinder<V> parameterBinder);
	
	/**
	 * Available identifier policies for entities.
	 * @see IdentifierInsertionManager
	 */
	@SuppressWarnings("java:S2326" /* Unused generics is necessary to caller signature (mapKey) to make policy identifier type match entity identifier one */)
	interface IdentifierPolicy<ID> {
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
		 * Meanwhile, this policy requires those instances to be capable of being marked as persisted (after insert to prevent the engine from
		 * trying to persist again an already persisted instance, for instance). A basic implementation can be a boolean switch on entity. 
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
	
	/**
	 * Contract for after-insert identifier generation policy. Since nothing needs to be configured for it, not method is added.
	 * @param <I> identifier type
	 */
	interface GeneratedKeysPolicy<I> extends IdentifierPolicy<I> {
		
	}
	
	class GeneratedKeysPolicySupport<I> implements GeneratedKeysPolicy<I> {
		
	}
	
	/**
	 * Contract for before-insert identifier generation policy. Requires the {@link Sequence} that generates the id, and that will be invoked before insertion.
	 * @param <I> identifier type
	 */
	interface BeforeInsertIdentifierPolicy<I> extends IdentifierPolicy<I> {
		
	}
	
	/**
	 * Before-insert identifier policy that will use given {@link Sequence} for all entities.
	 * 
	 * @param <I>
	 * @author Guillaume Mary
	 */
	class BeforeInsertIdentifierPolicySupport<I> implements BeforeInsertIdentifierPolicy<I> {
		
		private final Sequence<I> sequence;
		
		public BeforeInsertIdentifierPolicySupport(Sequence<I> sequence) {
			this.sequence = sequence;
		}
		
		public Sequence<I> getSequence() {
			return sequence;
		}
	}
	
	/**
	 * Default configuration to store sequence values for before-insert identifier policy
	 * 
	 * @author Guillaume Mary
	 */
	class PooledHiLoSequenceIdentifierPolicySupport implements BeforeInsertIdentifierPolicy<Long> {
		
		private final PooledHiLoSequenceStorageOptions storageOptions;
		
		public PooledHiLoSequenceIdentifierPolicySupport() {
			this.storageOptions = PooledHiLoSequenceStorageOptions.DEFAULT;
		}
		
		public PooledHiLoSequenceIdentifierPolicySupport(PooledHiLoSequenceStorageOptions sequenceStorageOptions) {
			this.storageOptions = sequenceStorageOptions;
		}
		
		public PooledHiLoSequenceStorageOptions getStorageOptions() {
			return storageOptions;
		}
	}

	/**
	 * Identifier pôlicy support for Database sequence (per entity)
	 */
	class DatabaseSequenceIdentifierPolicySupport implements BeforeInsertIdentifierPolicy<Long> {
		
		private final DatabaseSequenceNamingStrategy databaseSequenceNamingStrategy;
		private final DatabaseSequenceSettings databaseSequenceSettings;
		
		public DatabaseSequenceIdentifierPolicySupport() {
			this(DatabaseSequenceNamingStrategy.DEFAULT);
		}
		
		public DatabaseSequenceIdentifierPolicySupport(DatabaseSequenceNamingStrategy databaseSequenceNamingStrategy) {
			this(databaseSequenceNamingStrategy, new DatabaseSequenceSettings(1, 1));
		}
		
		public DatabaseSequenceIdentifierPolicySupport(DatabaseSequenceNamingStrategy databaseSequenceNamingStrategy, DatabaseSequenceSettings databaseSequenceSettings) {
			this.databaseSequenceNamingStrategy = databaseSequenceNamingStrategy;
			this.databaseSequenceSettings = databaseSequenceSettings;
		}
		
		public DatabaseSequenceNamingStrategy getDatabaseSequenceNamingStrategy() {
			return databaseSequenceNamingStrategy;
		}
		
		public DatabaseSequenceSettings getDatabaseSequenceSettings() {
			return databaseSequenceSettings;
		}
	}
	
	interface DatabaseSequenceNamingStrategy {
		
		String giveName(Class<?> entityType);
		
		DatabaseSequenceNamingStrategy HIBERNATE_DEFAULT = entityType -> Strings.capitalize(entityType.getSimpleName()) + "_seq";
		
		DatabaseSequenceNamingStrategy DEFAULT = HIBERNATE_DEFAULT; 
	}
	
	/**
	 * Contract for already-assigned identifier policy. Requires the methods that store entity persistence state.
	 * @param <I> identifier type
	 */
	interface AlreadyAssignedIdentifierPolicy<C, I> extends IdentifierPolicy<I> {
		
		Consumer<C> getMarkAsPersistedFunction();
		
		Function<C, Boolean> getIsPersistedFunction();
	}
	
	class AlreadyAssignedIdentifierPolicySupport<C, I> implements AlreadyAssignedIdentifierPolicy<C, I> {
		
		private final Consumer<C> markAsPersistedFunction;
		
		private final Function<C, Boolean> isPersistedFunction;
		
		public AlreadyAssignedIdentifierPolicySupport(Consumer<C> markAsPersistedFunction, Function<C, Boolean> isPersistedFunction) {
			this.markAsPersistedFunction = markAsPersistedFunction;
			this.isPersistedFunction = isPersistedFunction;
		}
		
		@Override
		public Consumer<C> getMarkAsPersistedFunction() {
			return markAsPersistedFunction;
		}
		
		@Override
		public Function<C, Boolean> getIsPersistedFunction() {
			return isPersistedFunction;
		}
	}
	
}
