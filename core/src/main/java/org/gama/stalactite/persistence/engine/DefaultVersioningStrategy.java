package org.gama.stalactite.persistence.engine;

import org.gama.reflection.PropertyAccessor;

/**
 * @author Guillaume Mary
 */
public abstract class DefaultVersioningStrategy<C> implements VersioningStrategy<C> {
	
	private final PropertyAccessor<Object, C> propertyAccessor;
	
	public DefaultVersioningStrategy(PropertyAccessor<Object, C> propertyAccessor) {
		this.propertyAccessor = propertyAccessor;
	}
	
	@Override
	public PropertyAccessor<Object, C> getPropertyAccessor() {
		return propertyAccessor;
	}
	
	@Override
	public C getVersion(Object o) {
		return propertyAccessor.get(o);
	}
	
	@Override
	public C upgrade(Object o) {
		C currentVersion = getVersion(o);
		C nextVersion = next(currentVersion);
		propertyAccessor.set(o, nextVersion);
		return currentVersion;
	}
	
	@Override
	public C revert(Object o, C previousValue) {
		C currentVersion = getVersion(o);
		propertyAccessor.set(o, previousValue);
		return currentVersion;
	}
	
	protected abstract C next(C previousVersion);
}
