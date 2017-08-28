package org.gama.stalactite.persistence.engine;

import org.gama.reflection.PropertyAccessor;

/**
 * @author Guillaume Mary
 * @param <C> the upgrade value type
 */
public interface VersioningStrategy<I, C> {
	
	PropertyAccessor<I, C> getPropertyAccessor();
	
	C getVersion(I o);
	
	C upgrade(I o);
	
	C revert(I o, C previousValue);
}
