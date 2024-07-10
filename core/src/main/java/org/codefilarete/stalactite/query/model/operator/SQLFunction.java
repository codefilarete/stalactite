package org.codefilarete.stalactite.query.model.operator;

import org.codefilarete.stalactite.query.model.Selectable;
import org.codefilarete.tool.collection.Arrays;

/**
 * Parent class that describes a function in some SQL statement
 * 
 * @param <V> java type of the function result (stands for Value)
 * @author Guillaume Mary
 */
public abstract class SQLFunction<V> implements Selectable<V> {
	
	private final String functionName;
	private final Class<V> javaType;
	private final Iterable<Object> arguments;
	
	protected SQLFunction(String functionName, Class<V> javaType, Object... arguments) {
		this(functionName, javaType, Arrays.asList(arguments));
	}
	
	protected SQLFunction(String functionName, Class<V> javaType, Iterable<? extends Object> arguments) {
		this.functionName = functionName;
		this.javaType = javaType;
		this.arguments = (Iterable<Object>) arguments;
	}
	
	public String getFunctionName() {
		return functionName;
	}
	
	public Iterable<Object> getArguments() {
		return arguments;
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
	public Class<V> getJavaType() {
		return javaType;
	}
}
