package org.codefilarete.stalactite.engine;

/**
 * @author Guillaume Mary
 */
public interface InsertExecutor<C> {
	
	void insert(Iterable<? extends C> entities);
}
