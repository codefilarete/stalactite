package org.gama.stalactite.query.model;

/**
 * @author Guillaume Mary
 */
public interface LimitAware {
	
	LimitChain limit(int value);
}
