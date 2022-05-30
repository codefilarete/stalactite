package org.codefilarete.stalactite.query.model;

/**
 * Contract for elements that can be union-ed 
 * 
 * @author Guillaume Mary
 */
public interface UnionAware {
	
	default Union unionAll(Query query) {
		return unionAll(() -> query);
	}
	
	Union unionAll(QueryProvider<Query> query);
}
