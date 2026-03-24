package org.codefilarete.stalactite.engine;

import org.codefilarete.reflection.PropertyAccessPoint;

/**
 * @author Guillaume Mary
 * @param <V> the upgrade value type
 */
public interface VersioningStrategy<C, V> {
	
	PropertyAccessPoint<C, V> getVersionAccessor();
	
	V getVersion(C o);
	
	V upgrade(C o);
	
	V revert(C o, V previousValue);
}
