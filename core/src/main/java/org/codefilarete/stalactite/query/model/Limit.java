package org.codefilarete.stalactite.query.model;

/**
 * @author Guillaume Mary
 */
public class Limit implements LimitChain<Limit> {
	
	private Integer value;
	
	public Integer getValue() {
		return value;
	}
	
	@Override
	public Limit setValue(Integer value) {
		this.value = value;
		return this;
	}
}
