package org.gama.stalactite.persistence.engine;

import org.gama.reflection.PropertyAccessor;

/**
 * @author Guillaume Mary
 * @param <C> the upgrade value type
 */
public interface VersioningStrategy<C> {
	
	PropertyAccessor<Object, C> getPropertyAccessor();
	
	C getVersion(Object o);
	
	C upgrade(Object o);
	
	C revert(Object o, C previousValue);
}
