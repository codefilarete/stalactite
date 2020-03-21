package org.gama.stalactite.persistence.engine;

/**
 * @author Guillaume Mary
 */
public interface IInsertExecutor<C> {
	
	int insert(Iterable<? extends C> entities);
}
