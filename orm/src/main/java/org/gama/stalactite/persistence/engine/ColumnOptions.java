package org.gama.stalactite.persistence.engine;

import java.sql.PreparedStatement;

import org.gama.lang.function.Sequence;

/**
 * @author Guillaume Mary
 */
public interface ColumnOptions<C, I> extends PropertyOptions {
	
	/**
	 * Defines the column as the identifier of the entity.
	 * 
	 * @param identifierPolicy an {@link IdentifierPolicy}
	 * @return the enclosing {@link IFluentEntityMappingBuilder}
	 */
	ColumnOptions<C, I> identifier(IdentifierPolicy identifierPolicy);
	
	/** Marks the property as mandatory. Note that using this method on an identifier one as no purpose because identifiers are already madatory. */
	ColumnOptions<C, I> mandatory();
	
	/**
	 * Available identifier policies for entities.
	 * @see org.gama.stalactite.persistence.id.manager.IdentifierInsertionManager
	 */
	interface IdentifierPolicy {
		/**
		 * Policy for entities that have their id given before insertion.
		 * <strong>Is only supported for entities that implement {@link org.gama.stalactite.persistence.id.Identified}</strong>
		 */
		IdentifierPolicy ALREADY_ASSIGNED = new IdentifierPolicy() {};
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
}
