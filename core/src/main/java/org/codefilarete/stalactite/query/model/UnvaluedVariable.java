package org.codefilarete.stalactite.query.model;

/**
 * Container of aa named placeholder value
 * 
 * @param <T> value type or type composing V
 * @param <V> value type
 * @author Guillaume Mary
 */
public class UnvaluedVariable<T, V> implements Variable<V> {
	
	private final String name;
	private final Class<T> valueType;
	
	public UnvaluedVariable(String name, Class<T> valueType) {
		this.name = name;
		this.valueType = valueType;
	}
	
	public String getName() {
		return name;
	}
	
	public Class<T> getValueType() {
		return valueType;
	}
}
