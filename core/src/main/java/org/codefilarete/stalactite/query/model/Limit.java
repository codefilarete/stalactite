package org.codefilarete.stalactite.query.model;

import org.codefilarete.stalactite.query.api.LimitChain;

/**
 * @author Guillaume Mary
 */
public class Limit implements LimitChain<Limit> {
	
	private Integer count;
	
	private Integer offset;
	
	public Limit() {
	}
	
	public Limit(Integer count) {
		this.count = count;
	}
	
	public Limit(Integer count, Integer offset) {
		this.count = count;
		this.offset = offset;
	}
	
	public Integer getCount() {
		return count;
	}
	
	public Integer getOffset() {
		return offset;
	}
	
	@Override
	public Limit setCount(Integer count) {
		return setCount(count, null);
	}
	
	@Override
	public Limit setCount(Integer value, Integer offset) {
		this.count = value;
		this.offset = offset;
		return this;
	}
}
