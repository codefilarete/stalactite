package org.gama.stalactite.persistence.engine;

/**
 * @author Guillaume Mary
 */
public interface ColumnOptions<T, I> {
	
	/**
	 * Defines the column as the identifier of the entity.
	 * 
	 * @param identifierPolicy an {@link IdentifierPolicy}
	 * @return the enclosing {@link IFluentMappingBuilder}
	 */
	IFluentMappingBuilder<T, I> identifier(IdentifierPolicy identifierPolicy);
	
	/**
	 * Available identifier policies for entities.
	 * Only {@link #ALREADY_ASSIGNED} is supported for now
	 * @see org.gama.stalactite.persistence.id.manager.IdentifierInsertionManager
	 */
	enum IdentifierPolicy {
		/**
		 * Policy for entities that have their id given before insertion.
		 * <strong>Is only supported for entities that implements {@link org.gama.stalactite.persistence.id.Identified}</strong>
		 */
		ALREADY_ASSIGNED,
		BEFORE_INSERT,
		AFTER_INSERT
	}
}
