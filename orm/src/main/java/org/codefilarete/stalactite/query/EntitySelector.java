package org.codefilarete.stalactite.query;

import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;

import org.codefilarete.stalactite.query.model.CriteriaChain;
import org.codefilarete.stalactite.query.model.Select;
import org.codefilarete.stalactite.query.model.Selectable;
import org.codefilarete.stalactite.sql.result.Accumulator;

/**
 * Contract that defines expected methods to load an entity graph conditionally on some properties criteria coming
 * from {@link CriteriaChain}.
 * 
 * @author Guillaume Mary
 */
public interface EntitySelector<C, I> {
	
	/**
	 * Loads entity graphs that matches given criteria.
	 * 
	 * <strong>
	 * Please note that the whole graph of matching entities is loaded : collections are fully loaded with all their
	 * elements, even those that don't match the criteria. 
	 * As long as there is an element that matches the criterion the entire collection is loaded, and the whole graph too.
	 * </strong>
	 *
	 * @param where some criteria for aggregate selection
	 * @return entities that match criteria
	 */
	Set<C> select(CriteriaChain where);
	
	/**
	 * Loads a projection that matches given criteria.
	 *
	 * @param selectAdapter the {@link Select} clause modifier
	 * @param accumulator the aggregator of the projection
	 * @param where some criteria for aggregate selection
	 * @return entities that match criteria
	 */
	<R, O> R selectProjection(Consumer<Select> selectAdapter, Accumulator<? super Function<Selectable<O>, O>, Object, R> accumulator, CriteriaChain where);
}
