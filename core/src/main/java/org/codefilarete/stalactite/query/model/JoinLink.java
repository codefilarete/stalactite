package org.codefilarete.stalactite.query.model;

/**
 * Contract for elements to be put in a join condition, in order to be transformed to SQL.
 * {@link #getJavaType Java type} is present to enforce joined elements' compatibility when used in {@link From}
 * 
 * @author Guillaume Mary
 */
public interface JoinLink<O> {
	
	Fromable getOwner();
	
	String getExpression();
	
	Class<O> getJavaType();
}
