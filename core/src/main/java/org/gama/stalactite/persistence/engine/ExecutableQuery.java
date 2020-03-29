package org.gama.stalactite.persistence.engine;

import java.util.List;

/**
 * Little interface to declare a {@link org.gama.stalactite.query.model.Query} as executable, see {@link org.gama.stalactite.persistence.engine.PersistenceContext.ExecutableSelect}
 * 
 * @param <C> type of object returned by query execution
 */
public interface ExecutableQuery<C> {
	
	List<C> execute();
	
}
