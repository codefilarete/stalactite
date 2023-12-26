package org.codefilarete.stalactite.query.model;

/**
 * @author Guillaume Mary
 */
public interface LimitAware<C> {
	
	C limit(int value);
}
