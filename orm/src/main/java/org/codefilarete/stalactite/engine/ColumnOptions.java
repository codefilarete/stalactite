package org.codefilarete.stalactite.engine;

import java.sql.PreparedStatement;
import java.util.function.Consumer;
import java.util.function.Function;

import org.codefilarete.stalactite.mapping.id.manager.IdentifierInsertionManager;
import org.codefilarete.stalactite.mapping.id.sequence.PooledHiLoSequence;
import org.codefilarete.stalactite.mapping.id.sequence.PooledHiLoSequenceOptions;
import org.codefilarete.stalactite.mapping.id.sequence.SequencePersister;
import org.codefilarete.stalactite.mapping.id.sequence.SequenceStorageOptions;
import org.codefilarete.stalactite.sql.ConnectionConfiguration;
import org.codefilarete.stalactite.sql.ConnectionProvider;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.statement.binder.ParameterBinder;
import org.codefilarete.tool.Reflections;
import org.codefilarete.tool.function.Converter;
import org.codefilarete.tool.function.Sequence;

/**
 * @author Guillaume Mary
 */
public interface ColumnOptions<O> extends PropertyOptions<O> {
	
	/**
	 * Marks the property as mandatory, which makes the mapped column not nullable : does not make a null checking at runtime.
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
		 * Policy for entities that have their id given by database after insert, such as increment column.
		 * This implies that generated values can be read through {@link PreparedStatement#getGeneratedKeys()}
		 * 
		 * @return a new policy that will be used to get the identifier value
		 */
		static <I> AfterInsertIdentifierPolicy<I> afterInsert() {
			return new AfterInsertIdentifierPolicySupport<>();
		}
		
		/**
		 * Policy for entities that want their id fixed just before insert which value is given by a {@link Sequence}.
		 * Reader may be interested in {@link PooledHiLoSequence}.
		 * Sequence data will be stored as specified through {@link SequenceStorageOptions#DEFAULT}
		 * 
		 * @return a new policy that will be used to get the identifier value
		 * @see #beforeInsert(SequenceStorageOptions) 
		 */
		static BeforeInsertIdentifierPolicy<Long> beforeInsert() {
			return new DefaultBeforeInsertIdentifierPolicySupport();
		}
		
		/**
		 * Policy for entities that want their id fixed just before insert which value is given by a {@link Sequence}.
		 * Reader may be interested in {@link PooledHiLoSequence}.
		 *
		 * @param sequenceStorageOptions the options about table to store sequence data
		 * @return a new policy that will be used to get the identifier value
		 * @see SequenceStorageOptions#DEFAULT
		 * @see SequenceStorageOptions#HIBERNATE_DEFAULT
		 */
		static BeforeInsertIdentifierPolicy<Long> beforeInsert(SequenceStorageOptions sequenceStorageOptions) {
			return new DefaultBeforeInsertIdentifierPolicySupport(sequenceStorageOptions);
		}
		
		/**
		 * Policy for entities that want their id fixed just before insert which value is given by a {@link Sequence}.
		 * Be aware that given sequence will be shared across all managed entities, meaning that, for instance,
		 * if it is a long sequence, an integer value can't be found twice in whole entities.   
		 * Reader may be interested in other {@link #beforeInsert()} methods to avoid such sharing behavior.
		 *
		 * @param sequence the {@link Sequence} to ask for identifier value
		 * @param <I> identifier type
		 * @return a new policy that will be used to get the identifier value
		 */
		static <I> BeforeInsertIdentifierPolicy<I> beforeInsert(Sequence<I> sequence) {
			return new BeforeInsertIdentifierPolicySupport<>(sequence);
		}
		
		/**
		 * Policy for entities that have their id already fixed and doesn't require the persistence engine to generate an identifier for them.
		 * Meanwhile, this policy requires those instances to be capable of being marked as persisted (after insert to prevent the engine from
		 * trying to persist again an already persisted instance, for instance). A basic implementation can be a boolean switch on entity. 
		 * 
		 * @param markAsPersistedFunction the {@link Consumer} that allows to mark the entity as "inserted in database"
		 * @param isPersistedFunction the {@link Function} that allows to know if entity was already inserted in database
		 * @param <C> entity type
		 * @param <I> identifier type
		 * @return a new policy that will be used to know persistent state of entities
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
	interface AfterInsertIdentifierPolicy<I> extends IdentifierPolicy<I> {
		
	}
	
	class AfterInsertIdentifierPolicySupport<I> implements AfterInsertIdentifierPolicy<I> {
		
	}
	
	/**
	 * Contract for before-insert identifier generation policy. Requires the {@link Sequence} that generates the id, and that will be invoked before insertion.
	 * @param <I> identifier type
	 */
	interface BeforeInsertIdentifierPolicy<I> extends IdentifierPolicy<I> {
		
		/**
		 * Expected to return a {@link Sequence} for given arguments
		 *
		 * @param entityType entity to get a sequence for
		 * @param connectionConfiguration elements to get access to database, its {@link ConnectionProvider} is a {@link SeparateTransactionExecutor}
		 * @param dialect useful to build DML statements that manage sequence value persistence
		 * @return
		 */
		Sequence<I> getIdentifierProvider(Class<?> entityType, ConnectionConfiguration connectionConfiguration, Dialect dialect);
	}
	
	/**
	 * Before-insert identifier policy that will used given {@link Sequence} for all entities.
	 * 
	 * @param <I>
	 * @author Guillaume Mary
	 */
	class BeforeInsertIdentifierPolicySupport<I> implements BeforeInsertIdentifierPolicy<I> {
		
		private final Sequence<I> identifierProvider;
		
		public BeforeInsertIdentifierPolicySupport(Sequence<I> identifierProvider) {
			this.identifierProvider = identifierProvider;
		}
		
		@Override
		public Sequence<I> getIdentifierProvider(Class<?> entityType, ConnectionConfiguration connectionConfiguration, Dialect dialect) {
			// we return sequence given at construction time 
			return identifierProvider;
		}
	}
	
	/**
	 * Default configuration to store sequence values for before-insert identifier policy
	 * 
	 * @author Guillaume Mary
	 */
	class DefaultBeforeInsertIdentifierPolicySupport implements BeforeInsertIdentifierPolicy<Long> {
		
		private final SequenceStorageOptions storageOptions;
		
		public DefaultBeforeInsertIdentifierPolicySupport() {
			this.storageOptions = SequenceStorageOptions.DEFAULT;
		}
		
		public DefaultBeforeInsertIdentifierPolicySupport(SequenceStorageOptions sequenceStorageOptions) {
			this.storageOptions = sequenceStorageOptions;
		}
		
		/**
		 * Overridden to dynamically build a {@link Sequence} for given arguments while using storage options given at
		 * construction time
		 */
		@Override
		public Sequence<Long> getIdentifierProvider(Class<?> entityType, ConnectionConfiguration connectionConfiguration, Dialect dialect) {
			PooledHiLoSequenceOptions options = new PooledHiLoSequenceOptions(50, entityType.getSimpleName());
			ConnectionProvider connectionProvider = connectionConfiguration.getConnectionProvider();
			if (!(connectionProvider instanceof SeparateTransactionExecutor)) {
				throw new MappingConfigurationException("Before-insert identifier policy configured with connection that doesn't support separate transaction,"
						+ " please provide a " + Reflections.toString(SeparateTransactionExecutor.class) + " as connection provider or change identifier policy");
			}
			return new PooledHiLoSequence(options,
					new SequencePersister(storageOptions, dialect, (SeparateTransactionExecutor) connectionProvider, connectionConfiguration.getBatchSize()));
		}
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
