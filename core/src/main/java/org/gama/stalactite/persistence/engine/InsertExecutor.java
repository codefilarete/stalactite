package org.gama.stalactite.persistence.engine;

/**
 * @author Guillaume Mary
 */
public interface InsertExecutor<C> {
	
	int insert(Iterable<? extends C> entities);
}
