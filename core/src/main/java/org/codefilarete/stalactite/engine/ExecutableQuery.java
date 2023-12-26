package org.codefilarete.stalactite.engine;

import org.codefilarete.stalactite.sql.result.Accumulator;

/**
 * Little interface to declare a {@link org.codefilarete.stalactite.query.model.Query} as executable.
 * 
 * @param <C> type of object returned by query execution
 * @see PersistenceContext.ExecutableBeanPropertyKeyQueryMapper
 * @see EntityPersister.ExecutableEntityQuery
 */
public interface ExecutableQuery<C> {
	
	/**
	 * Will run underlying {@link org.codefilarete.stalactite.query.model.Query} and executes given {@link Accumulator}
	 * on its result.
	 * 
	 * @param accumulator query result finalizer
	 * @return beans found by the query and finalized by accumulator
	 * @param <R> result type
	 */
	<R> R execute(Accumulator<C, ?, R> accumulator);
	
}