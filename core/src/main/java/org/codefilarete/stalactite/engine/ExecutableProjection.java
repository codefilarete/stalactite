package org.codefilarete.stalactite.engine;

import java.util.function.Function;

import org.codefilarete.stalactite.query.model.Selectable;
import org.codefilarete.stalactite.sql.result.Accumulator;

/**
 * Little interface to declare a projection as executable.
 * 
 * @see PersistenceContext.ExecutableBeanPropertyKeyQueryMapper
 * @see EntityPersister.ExecutableEntityQuery
 */
public interface ExecutableProjection {
	
	/**
	 * Marks this projection to apply the <code>distinct</code> SQL keyword
	 * @return the current instance to chain with other methods
	 */
	ExecutableProjection distinct();
	
	/**
	 * Will run an underlying projection and executes given {@link Accumulator} on its result.
	 * 
	 * @param accumulator projection result finalizer, {@link java.sql.ResultSet} values are read through the given {@link Function}
	 * @return beans found by the query and finalized by accumulator
	 * @param <R> result type
	 * @param <O> intermediary type that helps to read the values coming form the database 
	 */
	<R, O> R execute(Accumulator<? super Function<Selectable<O>, O>, ?, R> accumulator);
	
}