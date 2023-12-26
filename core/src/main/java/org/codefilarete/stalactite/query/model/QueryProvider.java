package org.codefilarete.stalactite.query.model;

/**
 * Simple contract for classes that can provide a {@link Query}. Made for fluent API elements to give the enclosing {@link Query}, so caller can get
 * it at any moment while writing its query without keeping a reference on initial {@link Query} instance.
 * 
 * @author Guillaume Mary
 */
public interface QueryProvider<T extends QueryStatement> {
	
	T getQuery();
}
