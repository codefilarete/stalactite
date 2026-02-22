package org.codefilarete.stalactite.query.model;

import org.codefilarete.stalactite.query.api.Variable;

/**
 * Parent class for operators with a single comparison value
 * 
 * @author Guillaume Mary
 */
public abstract class UnitaryOperator<V> extends ConditionalOperator<V, V> {
	
	/** Value of the operator */
	private Variable<V> value;
	
	protected UnitaryOperator() {
	}
	
	protected UnitaryOperator(Variable<V> value) {
		this.value = value;
	}
	
	protected UnitaryOperator(V value) {
		this.value = new ValuedVariable<>(value);
	}
	
	/**
	 * @return the value of this operator
	 */
	public Variable<V> getValue() {
		return value;
	}
	
	/**
	 * Sets the value of this operator
	 * @param value the new value
	 */
	@Override
	public void setValue(Variable<V> value) {
		this.value = value;
	}
	
	@Override
	public boolean isNull() {
		return this.value instanceof ValuedVariable && ((ValuedVariable<V>) this.value).getValue() == null;
	}
}
