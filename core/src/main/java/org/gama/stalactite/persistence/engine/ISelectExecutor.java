package org.gama.stalactite.persistence.engine;

import java.util.List;

/**
 * Simple interface defining what's expected from selector 
 * 
 * @author Guillaume Mary
 */
public interface ISelectExecutor<C, I> {
	
	List<C> select(Iterable<I> ids);
}
