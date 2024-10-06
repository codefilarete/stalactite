package org.codefilarete.stalactite.query.model;

import org.codefilarete.stalactite.query.model.ValueWrapper.RawValueWrapper;

/**
 * Parent class for operators that don't need a comparison value
 * 
 * @author Guillaume Mary
 */
public abstract class UnitaryOperator<V> extends ConditionalOperator<V, V> {
	
	/** Value of the operator */
	private ValueWrapper<V> value = new RawValueWrapper<>();
	
	public UnitaryOperator() {
	}
	
	public UnitaryOperator(V value) {
		this.value.setValue(value);
	}
	
	public UnitaryOperator(ValueWrapper<V> value) {
		this.value = value;
	}
	
	/**
	 * @return the value of this operator
	 */
	public V getValue() {
		return value.getValue();
	}
	
	/**
	 * Sets the value of this operator
	 * @param value the new value
	 */
	@Override
	public void setValue(V value) {
		this.value.setValue(value);
	}
	
	public ValueWrapper<V> getValueWrapper() {
		return this.value;
	}
	
	public void setValueWrapper(ValueWrapper<V> value) {
		this.value = value;
	}
	
	@Override
	public boolean isNull() {
		return getValue() == null;
	}
}
