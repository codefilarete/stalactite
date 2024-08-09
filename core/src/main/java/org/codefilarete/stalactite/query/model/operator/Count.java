package org.codefilarete.stalactite.query.model.operator;

import org.codefilarete.stalactite.query.model.Selectable;

/**
 * Represents a count operation (on a column)
 * 
 * @author Guillaume Mary
 */
public class Count extends SQLFunction<Long> {
	
	private boolean distinct;
	
	public Count(Selectable<?>... values) {
		super("count", Long.class, (Object[]) values);
	}
	
	public Count(Iterable<? extends Selectable<?>> values) {
		super("count", Long.class, values);
	}
	
	/**
	 * Marks this instance to use <code>distinct</code> SQL keyword onto its values
	 * @return the current instance to chain with other methods
	 */
	public Count distinct() {
		this.distinct = true;
		return this;
	}
	
	/**
	 * Marks this instance to eventually use <code>distinct</code> SQL keyword onto its values
	 * @param distinct true to use <code>distinct</code> SQL keyword
	 * @return the current instance to chain with other methods
	 */
	public Count setDistinct(boolean distinct) {
		this.distinct = distinct;
		return this;
	}
	
	/**
	 * Indicates if this instance should use <code>distinct</code> SQL keyword onto its values
	 * @return true if this should use <code>distinct</code>
	 */
	public boolean isDistinct() {
		return distinct;
	}
}
