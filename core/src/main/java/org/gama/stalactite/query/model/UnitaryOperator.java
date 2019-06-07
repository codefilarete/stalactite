package org.gama.stalactite.query.model;

/**
 * @author Guillaume Mary
 */
public class UnitaryOperator<V> extends AbstractOperator<V> {
	
	/** Value of the operator */
	private V value;
	
	public UnitaryOperator(V value) {
		this.value = value;
	}
	
	/**
	 * @return the value of this operand
	 */
	public V getValue() {
		return value;
	}
	
	/**
	 * Sets the value of this operant
	 * @param value the new value
	 */
	public void setValue(V value) {
		this.value = value;
	}
	
	@Override
	public boolean isNull() {
		return getValue() == null;
	}
}
