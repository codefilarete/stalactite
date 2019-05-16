package org.gama.stalactite.persistence.engine;

import org.gama.reflection.IReversibleAccessor;

/**
 * @author Guillaume Mary
 * @param <C> the upgrade value type
 */
public interface VersioningStrategy<I, C> {
	
	IReversibleAccessor<I, C> getVersionAccessor();
	
	C getVersion(I o);
	
	C upgrade(I o);
	
	C revert(I o, C previousValue);
}
