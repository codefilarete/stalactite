package org.gama.stalactite.persistence.mapping;

import org.gama.reflection.PropertyAccessor;
import org.gama.stalactite.persistence.id.manager.IdentifierInsertionManager;

/**
 * @author Guillaume Mary
 */
public class IdMappingStrategy<T, I> {
	
	public static <T, I> IIdAccessor<T, I> toIdAccessor(PropertyAccessor<T, I> propertyAccessor) {
		return new IIdAccessor<T, I>() {
			
			@Override
			public I getId(T t) {
				return propertyAccessor.get(t);
			}
			
			@Override
			public void setId(T t, I identifier) {
				propertyAccessor.set(t, identifier);
			}
		};
	}
	
	private final IIdAccessor<T, I> idAccessor;
	
	private final IdentifierInsertionManager<T> identifierInsertionManager;
	
	public IdMappingStrategy(IIdAccessor<T, I> idAccessor, IdentifierInsertionManager<T> identifierInsertionManager) {
		this.idAccessor = idAccessor;
		this.identifierInsertionManager = identifierInsertionManager;
	}
	
	public IdMappingStrategy(PropertyAccessor<T, I> identifierAccessor, IdentifierInsertionManager<T> identifierInsertionManager) {
		this(toIdAccessor(identifierAccessor), identifierInsertionManager);
	}
	
	public IdentifierInsertionManager<T> getIdentifierInsertionManager() {
		return identifierInsertionManager;
	}
	
	public I getId(T t) {
		return idAccessor.getId(t);
	}
	
	public void setId(T t, I identifier) {
		idAccessor.setId(t, identifier);
	}
}
