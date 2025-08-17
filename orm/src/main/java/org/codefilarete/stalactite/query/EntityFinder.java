package org.codefilarete.stalactite.query;

import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;

import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree;
import org.codefilarete.stalactite.engine.runtime.query.EntityQueryCriteriaSupport;
import org.codefilarete.stalactite.query.model.CriteriaChain;
import org.codefilarete.stalactite.query.model.Limit;
import org.codefilarete.stalactite.query.model.OrderBy;
import org.codefilarete.stalactite.query.model.Select;
import org.codefilarete.stalactite.query.model.Selectable;
import org.codefilarete.stalactite.sql.result.Accumulator;
import org.codefilarete.stalactite.sql.statement.SQLOperation.SQLOperationListener;

/**
 * Contract that defines the methods to load an entity graph conditionally on some property criteria coming
 * from {@link CriteriaChain}.
 * 
 * @author Guillaume Mary
 */
public interface EntityFinder<C, I> {
	
	/**
	 * Loads some entities that match given criteria.
	 * 
	 * <strong>
	 * Please note that the whole graph of matching entities is loaded: collections are fully loaded with all their
	 * elements, even those that don't match the criteria.
	 * As long as there is an element that matches the criterion the entire collection is loaded, and the whole graph too.
	 * </strong>
	 *
	 * @param where some criteria for aggregate selection
	 * @param orderBy the order-by clause to apply to the final query
	 * @param limit the limit clause to apply to the final query
	 * @param valuesPerParam values presents in criteria per their name, may be empty
	 * @return entities that match criteria
	 */
	Set<C> select(ConfiguredEntityCriteria where,
				  OrderBy orderBy,
				  Limit limit,
				  Map<String, Object> valuesPerParam);
	
	/**
	 * Loads a projection that matches given criteria.
	 *
	 * @param selectAdapter the {@link Select} clause modifier
	 * @param accumulator the aggregator of the projection
	 * @param where some criteria for aggregate selection
	 * @param distinct either to add or not the "distinct" keyword to the select clause
	 * @param orderBy the order-by clause to apply to the final query
	 * @param limit the limit clause to apply to the final query
	 * @return entities that match criteria
	 */
	<R, O> R selectProjection(Consumer<Select> selectAdapter,
							  Accumulator<? super Function<Selectable<O>, O>, Object, R> accumulator,
							  ConfiguredEntityCriteria where,
							  boolean distinct,
							  OrderBy orderBy,
							  Limit limit);
	
	EntityJoinTree<C, I> getEntityJoinTree();
	
	EntityQueryCriteriaSupport<C, I> newCriteriaSupport();
	
	void setOperationListener(SQLOperationListener<?> operationListener);
}
