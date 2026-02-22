package org.codefilarete.stalactite.query.api;

import org.codefilarete.stalactite.query.model.From;

/**
 * Contract for elements to be put in a join condition, in order to be transformed to SQL.
 * {@link #getJavaType Java type} is present to enforce joined elements' compatibility when used in {@link From}
 * 
 * @author Guillaume Mary
 */
public interface JoinLink<T extends Fromable, O> extends Selectable<O> {
	
	T getOwner();
	
	String getExpression();
	
	Class<O> getJavaType();
}
