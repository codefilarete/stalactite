package org.codefilarete.stalactite.query.model;

/**
 * @author Guillaume Mary
 */
public interface LimitChain<SELF extends LimitChain<SELF>> {
	
	SELF setCount(Integer count);
	
	SELF setCount(Integer value, Integer offset);
}
