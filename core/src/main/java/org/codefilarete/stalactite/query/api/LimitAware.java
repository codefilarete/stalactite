package org.codefilarete.stalactite.query.api;

/**
 * @author Guillaume Mary
 */
public interface LimitAware<C> {
	
	C limit(int value);
	
	C limit(int value, Integer offset);
}
