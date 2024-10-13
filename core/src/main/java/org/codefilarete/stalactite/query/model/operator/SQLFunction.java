package org.codefilarete.stalactite.query.model.operator;

import org.codefilarete.stalactite.query.model.Selectable;

/**
 * Parent class that describes a function in some SQL statement
 * 
 * @param <V> value type, can be a simple class as String or Integer or a complex one as {@link org.codefilarete.stalactite.query.model.operator.SQLFunction}
 * @param <O> java type of the function result
 * @author Guillaume Mary
 */
public abstract class SQLFunction<V, O> implements Selectable<O> {
	
	private final String functionName;
	private final Class<O> javaType;
	/** Value of argument */
	private V value;
	
	protected SQLFunction(String functionName, Class<O> javaType) {
		this(functionName, javaType, null);
	}
	
	protected SQLFunction(String functionName, Class<O> javaType, V value) {
		this.functionName = functionName;
		this.javaType = javaType;
		this.value = value;
	}
	
	public String getFunctionName() {
		return functionName;
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
	public void setValue(V value) {
		this.value = value;
	}
	
	/**
	 * Returns the function name
	 * @return the function name
	 */
	@Override
	public String getExpression() {
		return functionName;
	}
	
	@Override
	public Class<O> getJavaType() {
		return javaType;
	}
	
	public Class<O> getType() {
		return javaType;
	}
}
