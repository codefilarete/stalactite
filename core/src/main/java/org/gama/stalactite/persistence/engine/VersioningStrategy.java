package org.gama.stalactite.persistence.engine;

import org.gama.reflection.IReversibleAccessor;

/**
 * @author Guillaume Mary
 * @param <V> the upgrade value type
 */
public interface VersioningStrategy<C, V> {
	
	IReversibleAccessor<C, V> getVersionAccessor();
	
	V getVersion(C o);
	
	V upgrade(C o);
	
	V revert(C o, V previousValue);
}
