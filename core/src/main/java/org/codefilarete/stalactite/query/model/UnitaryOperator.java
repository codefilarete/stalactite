package org.codefilarete.stalactite.query.model;

/**
 * Parent class for operators with a single comparison value
 * 
 * @author Guillaume Mary
 */
public abstract class UnitaryOperator<V> extends ConditionalOperator<V, V> {
	
	/** Value of the operator */
	private V value;
	
	protected UnitaryOperator() {
	}
	
	protected UnitaryOperator(V value) {
		this.value = value;
	}
	
	/**
	 * @return the value of this operator
	 */
	public V getValue() {
		return value;
	}
	
	/**
	 * Sets the value of this operator
	 * @param value the new value
	 */
	@Override
	public void setValue(V value) {
		this.value = value;
	}
	
	@Override
	public boolean isNull() {
		return getValue() == null;
	}
}
