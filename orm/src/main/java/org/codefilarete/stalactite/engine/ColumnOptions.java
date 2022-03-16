package org.codefilarete.stalactite.engine;

import java.sql.PreparedStatement;
import java.util.function.Consumer;
import java.util.function.Function;

import org.codefilarete.stalactite.mapping.id.manager.IdentifierInsertionManager;
import org.codefilarete.stalactite.mapping.id.sequence.PooledHiLoSequence;
import org.codefilarete.tool.function.Sequence;

/**
 * @author Guillaume Mary
 */
public interface ColumnOptions<C, I> extends PropertyOptions {
	
	/**
	 * Marks the property as mandatory, which makes the mapped column not nullable : does not make a null checking at runtime.
	 * Note that using this method on an identifier one as no purpose because identifiers are already mandatory.
	 */
	@Override
	ColumnOptions<C, I> mandatory();
	
	@Override
	ColumnOptions<C, I> setByConstructor();
	
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
		
		Sequence<I> getIdentifierProvider();
	}
	
	class BeforeInsertIdentifierPolicySupport<I> implements BeforeInsertIdentifierPolicy<I> {
		
		private final Sequence<I> identifierProvider;
		
		public BeforeInsertIdentifierPolicySupport(Sequence<I> identifierProvider) {
			this.identifierProvider = identifierProvider;
		}
		
		@Override
		public Sequence<I> getIdentifierProvider() {
			return identifierProvider;
		}
	}
	
	/**
	 * Contract for already-assigned identifier policy. Requires the methods that store entities persistence state.
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
