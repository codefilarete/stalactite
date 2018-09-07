package org.gama.stalactite.persistence.mapping;

import org.gama.reflection.IReversibleAccessor;

/**
 * Interface for general access to the identifier of an entity
 * 
 * @author Guillaume Mary
 */
public interface IdAccessor<C, I> {
	
	/**
	 * Gets an entity identifier.
	 * Used for SQL write orders for instance, in where clause, to target the right entity
	 * 
	 * @param c any entity
	 * @return the entity identifier
	 */
	I getId(C c);
	
	/**
	 * Sets entity identifier.
	 * Used on very first time persistence of the entity in conjonction with {@link org.gama.stalactite.persistence.id.manager.IdentifierInsertionManager}
	 * 
	 * @param c an entity
	 * @param identifier the generated identifier
	 */
	void setId(C c, I identifier);
	
	/**
	 * Creates a bridge between {@link IReversibleAccessor} and an {@link IdAccessor}
	 *
	 * @param reversibleAccessor any {@link IReversibleAccessor}
	 * @param <T> type of target bean
	 * @param <I> type of the read property
	 * @return an {@link IdAccessor} that mimics the given {@link IReversibleAccessor}
	 */
	static <T, I> IdAccessor<T, I> idAccessor(IReversibleAccessor<T, I> reversibleAccessor) {
		return new IdAccessor<T, I>() {
			@Override
			public I getId(T t) {
				return reversibleAccessor.get(t);
			}
			
			@Override
			public void setId(T t, I identifier) {
				reversibleAccessor.toMutator().set(t, identifier);
			}
		};
	}
}
