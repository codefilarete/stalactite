package org.gama.stalactite.persistence.engine;

/**
 * @author Guillaume Mary
 */
public interface InsertExecutor<C> {
	
	void insert(Iterable<? extends C> entities);
}
