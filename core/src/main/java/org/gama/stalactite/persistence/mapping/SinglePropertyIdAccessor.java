package org.gama.stalactite.persistence.mapping;

import org.gama.reflection.ReversibleAccessor;

/**
 * An {@link IdAccessor} dedicated to single-property id.
 * 
 * @param <T> entity type
 * @param <I> identifier type
 */
public final class SinglePropertyIdAccessor<T, I> implements IdAccessor<T, I> {
	
	private final ReversibleAccessor<T, I> idAccessor;
	
	public SinglePropertyIdAccessor(ReversibleAccessor<T, I> idAccessor) {
		this.idAccessor = idAccessor;
	}
	
	public ReversibleAccessor<T, I> getIdAccessor() {
		return idAccessor;
	}
	
	@Override
	public I getId(T t) {
		return idAccessor.get(t);
	}
	
	@Override
	public void setId(T t, I identifier) {
		idAccessor.toMutator().set(t, identifier);
	}
}
