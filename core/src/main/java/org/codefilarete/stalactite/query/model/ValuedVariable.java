package org.codefilarete.stalactite.query.model;

/**
 * Wrapper for String, Integer, etc values of operators.
 * 
 * @param <V>
 * @author Guillaume Mary
 */
public class ValuedVariable<V> implements Variable<V> {
	
	private V value;
	
	public ValuedVariable() {
	}
	
	public ValuedVariable(V value) {
		this.value = value;
	}
	
	public void setValue(V value) {
		this.value = value;
	}
	
	public V getValue() {
		return value;
	}
}
