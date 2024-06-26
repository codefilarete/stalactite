package org.codefilarete.stalactite.query.model;

/**
 * Parent class for operators that don't need a comparison value
 * 
 * @author Guillaume Mary
 */
public abstract class UnitaryOperator<V> extends ConditionalOperator<V, V> {
	
	/** Value of the operator */
	private V value;
	
	public UnitaryOperator() {
	}
	
	public UnitaryOperator(V value) {
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
