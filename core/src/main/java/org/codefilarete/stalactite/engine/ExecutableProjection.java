package org.codefilarete.stalactite.engine;

import java.util.function.Consumer;
import java.util.function.Function;

import org.codefilarete.stalactite.engine.EntityCriteria.CriteriaPath;
import org.codefilarete.stalactite.query.model.Select;
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
	 * Adds a select inspector to this projection.
	 * Made to collect information about the selectable available in the select clause.
	 * The given consumer is not supposed to modify the select clause in here because the {@link ExecutableProjection} has been built earlier with
	 * a select clause adapter. Thus making it again is useless. As a consequence, any modification to the provided {@link Select} has non effect
	 * on the final select clause.
	 * 
	 * @param select the select clause adapter that will get a copy of the final Select clause
	 */
	void selectInspector(Consumer<Select> select);
	
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
	 */
	<R> R execute(Accumulator<? super ProjectionDataProvider, ?, R> accumulator);
	
	interface ProjectionDataProvider {
		
		<O> O getValue(Selectable<O> selectable);
		
		<O> O getValue(CriteriaPath<?, O> selectable);
	}
}
