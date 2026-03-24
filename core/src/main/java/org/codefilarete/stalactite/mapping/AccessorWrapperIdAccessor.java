package org.codefilarete.stalactite.mapping;

import org.codefilarete.reflection.ReadWritePropertyAccessPoint;
import org.codefilarete.reflection.ReversibleAccessor;

/**
 * An {@link IdAccessor} wrapping a {@link ReversibleAccessor}
 * 
 * @param <T> entity type
 * @param <I> identifier type
 */
public final class AccessorWrapperIdAccessor<T, I> implements IdAccessor<T, I> {
	
	private final ReadWritePropertyAccessPoint<T, I> idAccessor;
	
	public AccessorWrapperIdAccessor(ReadWritePropertyAccessPoint<T, I> idAccessor) {
		this.idAccessor = idAccessor;
	}
	
	public ReadWritePropertyAccessPoint<T, I> getIdAccessor() {
		return idAccessor;
	}
	
	@Override
	public I getId(T t) {
		return idAccessor.get(t);
	}
	
	@Override
	public void setId(T t, I identifier) {
		idAccessor.set(t, identifier);
	}
}
