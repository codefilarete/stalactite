package org.gama.stalactite.persistence.engine;

import java.sql.PreparedStatement;
import java.util.function.Consumer;
import java.util.function.Function;

import org.gama.lang.function.Sequence;

/**
 * @author Guillaume Mary
 */
public interface ColumnOptions<C, I> extends PropertyOptions {
	
	/**
	 * Defines the column as the identifier of the entity.
	 * 
	 * @param identifierPolicy an {@link IdentifierPolicy}
	 * @return the enclosing {@link FluentEntityMappingBuilder}
	 */
	ColumnOptions<C, I> identifier(IdentifierPolicy identifierPolicy);
	
	/** Marks the property as mandatory. Note that using this method on an identifier one as no purpose because identifiers are already madatory. */
	@Override
	ColumnOptions<C, I> mandatory();
	
	@Override
	ColumnOptions setByConstructor();
	
	/**
	 * Available identifier policies for entities.
	 * @see org.gama.stalactite.persistence.id.manager.IdentifierInsertionManager
	 */
	interface IdentifierPolicy {
		/**
		 * Policy for entities that have their id given by database after insert, such as increment column.
		 * This implies that generated values can be read through {@link PreparedStatement#getGeneratedKeys()}
		 */
		IdentifierPolicy AFTER_INSERT = new IdentifierPolicy() {};
		
		/**
		 * Policy for entities that want their id fixed just before insert which value is given by a {@link Sequence}.
		 * Reader may be interested by {@link org.gama.stalactite.persistence.id.sequence.PooledHiLoSequence}.
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
		 * @return a new policy taht will be used to know persistent state of entities
		 */
		static <C, I> AlreadyAssignedIdentifierPolicy<C, I> alreadyAssigned(Consumer<C> markAsPersistedFunction, Function<C, Boolean> isPersistedFunction) {
			return new AlreadyAssignedIdentifierPolicySupport<>(markAsPersistedFunction, isPersistedFunction);
		}
	}
	
	interface BeforeInsertIdentifierPolicy<I> extends IdentifierPolicy {
		
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
	
	interface AlreadyAssignedIdentifierPolicy<C, I> extends IdentifierPolicy {
		
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
