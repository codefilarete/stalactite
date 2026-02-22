package org.codefilarete.stalactite.query.model;

import org.codefilarete.stalactite.query.api.Variable;

/**
 * General contract for SQL operator such as in, =, <, >, like, between, etc. 
 * 
 * @param <T> value type or type composing V
 * @param <V> value type
 * @author Guillaume Mary
 */
@SuppressWarnings("squid:S2326")	// T is voluntary left, even if not used in this class, for using-API
public abstract class ConditionalOperator<T, V> {
	
	/** Is this operator must be negated ? */
	private boolean not;
	
	/**
	 * @return true if this operator uses "not"
	 */
	public boolean isNot() {
		return not;
	}
	
	/**
	 * Sets "not" value
	 * @param not true for this operator to use "not", false to let it use normal operator
	 */
	public void setNot(boolean not) {
		this.not = not;
	}
	
	/**
	 * Negates this operator
	 */
	public ConditionalOperator<T, V> not() {
		return not(true);
	}
	
	/**
	 * Switch this operator negation and return this
	 * @param not the new value of the negation
	 */
	public ConditionalOperator<T, V> not(boolean not) {
		setNot(not);
		return this;
	}
	
	/**
	 * Reverses logical operator
	 */
	public void switchNot() {
		this.not = !this.not;
	}
	
	public abstract void setValue(Variable<V> value);
	
	public final void setValue(V value) {
		setValue(new ValuedVariable<>(value));
	}
	
	public abstract boolean isNull();
}
