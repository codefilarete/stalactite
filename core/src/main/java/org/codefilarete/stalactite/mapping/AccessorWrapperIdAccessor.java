package org.codefilarete.stalactite.mapping;

import org.codefilarete.reflection.ReadWritePropertyAccessPoint;
import org.codefilarete.reflection.ReversibleAccessor;

/**
 * An {@link IdAccessor} wrapping a {@link ReversibleAccessor}
 * 
 * @param <C> entity type
 * @param <I> identifier type
 */
public final class AccessorWrapperIdAccessor<C, I> implements IdAccessor<C, I> {
	
	private final ReadWritePropertyAccessPoint<C, I> idAccessor;
	
	public AccessorWrapperIdAccessor(ReadWritePropertyAccessPoint<C, I> idAccessor) {
		this.idAccessor = idAccessor;
	}
	
	public ReadWritePropertyAccessPoint<C, I> getIdAccessor() {
		return idAccessor;
	}
	
	@Override
	public I getId(C c) {
		return idAccessor.get(c);
	}
	
	@Override
	public void setId(C c, I identifier) {
		idAccessor.set(c, identifier);
	}
}
