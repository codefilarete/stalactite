package org.codefilarete.stalactite.engine;

import java.util.List;

/**
 * Little interface to declare a {@link org.codefilarete.stalactite.query.model.Query} as executable, see {@link PersistenceContext.ExecutableSelect}
 * 
 * @param <C> type of object returned by query execution
 */
public interface ExecutableQuery<C> {
	
	List<C> execute();
	
}
