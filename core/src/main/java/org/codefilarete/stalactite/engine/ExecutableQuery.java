package org.codefilarete.stalactite.engine;

import java.util.List;
import java.util.Set;

/**
 * Little interface to declare a {@link org.codefilarete.stalactite.query.model.Query} as executable, see {@link PersistenceContext.ExecutableSelect}
 * 
 * @param <C> type of object returned by query execution
 */
public interface ExecutableQuery<C> {
	
	Set<C> execute();
	
}
