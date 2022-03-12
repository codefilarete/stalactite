package org.codefilarete.stalactite.query.model;

/**
 * Implementing classes should return the whole query being created
 * 
 * @author Guillaume Mary
 */
public interface QueryProvider {
	
	Query getQuery();
}
