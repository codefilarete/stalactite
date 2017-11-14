package org.gama.stalactite.query.model;

/**
 * @author Guillaume Mary
 */
public interface LimitChain<SELF extends LimitChain<SELF>> {
	
	SELF setValue(Integer value);
}
