package org.codefilarete.stalactite.query.model;

import org.codefilarete.stalactite.query.api.Variable;

/**
 * Container of a named placeholder : a variable that doesn't have a value yet
 * 
 * @param <T> value type or type composing V
 * @param <V> value type
 * @author Guillaume Mary
 */
public class Placeholder<T, V> implements Variable<V> {
	
	private final String name;
	private final Class<T> valueType;
	
	public Placeholder(String name, Class<T> valueType) {
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
