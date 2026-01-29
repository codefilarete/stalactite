package org.codefilarete.stalactite.spring.repository.query.projection;

import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;

import org.codefilarete.reflection.AccessorByMember;
import org.codefilarete.reflection.AccessorChain;
import org.codefilarete.reflection.Accessors;
import org.codefilarete.stalactite.engine.EntityPersister.ExecutableProjectionQuery;
import org.codefilarete.stalactite.engine.EntityCriteria.OrderByChain.Order;
import org.codefilarete.stalactite.engine.runtime.ProjectionQueryCriteriaSupport;
import org.codefilarete.stalactite.engine.runtime.ProjectionQueryCriteriaSupport.ProjectionQueryPageSupport;
import org.codefilarete.stalactite.query.model.JoinLink;
import org.codefilarete.stalactite.query.model.Limit;
import org.codefilarete.stalactite.query.model.Selectable;
import org.codefilarete.stalactite.spring.repository.query.execution.AbstractQueryExecutor;
import org.codefilarete.stalactite.spring.repository.query.StalactiteQueryMethod;
import org.codefilarete.stalactite.spring.repository.query.execution.StalactiteQueryMethodInvocationParameters;
import org.codefilarete.stalactite.sql.result.Accumulator;
import org.springframework.data.domain.Sort.Direction;
import org.springframework.data.repository.query.Parameter;

import static org.codefilarete.stalactite.spring.repository.query.execution.AbstractRepositoryQuery.buildAliases;

/**
 * Implementation of {@link AbstractQueryExecutor} dedicated to projections.
 * Implementation is based on {@link ProjectionQueryCriteriaSupport}: a copy from the default one is made at construction time to have a dedicated
 * instance for each derived query.
 *
 * @param <C> domain entity type
 * @author Guillaume Mary
 */
class ProjectionQueryExecutor<C> extends AbstractQueryExecutor<List<Object>, Object> {
	
	private final ProjectionQueryCriteriaSupport<C, ?> projectionQueryCriteriaSupport;
	private final Accumulator<Function<Selectable<Object>, Object>, List<Map<String, Object>>, List<Map<String, Object>>> accumulator;
	
	public ProjectionQueryExecutor(StalactiteQueryMethod method,
								   ProjectionQueryCriteriaSupport<C, ?> defaultProjectionQueryCriteriaSupport,
								   IdentityHashMap<JoinLink<?, ?>, AccessorChain<C, ?>> columnToProperties) {
		super(method);
		IdentityHashMap<JoinLink<?, ?>, String> aliases = buildAliases(columnToProperties);
		// we "clone" the default projection query to make our own, dedicated to the derived query
		this.projectionQueryCriteriaSupport = defaultProjectionQueryCriteriaSupport.copyFor(select -> {
			select.clear();
			columnToProperties.keySet().forEach(selectable -> {
				select.add(selectable, aliases.get(selectable));
			});
		});
		this.accumulator = new TupleAccumulator(columnToProperties);
	}
	
	@Override
	public Supplier<List<Object>> buildQueryExecutor(StalactiteQueryMethodInvocationParameters invocationParameters) {
		return () -> {
			ExecutableProjectionQuery<C, ?> projectionQuery = handleDynamicParameters(invocationParameters, projectionQueryCriteriaSupport);
			
			int i = 1;
			for (Parameter bindableParameter : invocationParameters.getParameters().getBindableParameters()) {
				Object value = invocationParameters.getBindableValue(bindableParameter.getIndex());
				// Note that we don't apply the value to the criteria of the derived query template, else we temper with it for next execution
				// defaultDerivedQuery.criteriaChain.criteria.get(i).setValue(..);
				projectionQuery.set(String.valueOf(i++), value);
			}
			
			return (List<Object>) (List) projectionQuery.execute(accumulator);
		};
	}
	
	private ExecutableProjectionQuery<C, ?> handleDynamicParameters(StalactiteQueryMethodInvocationParameters invocationParameters,
																	ProjectionQueryCriteriaSupport<C, ?> actualProjectionQueryCriteriaSupport) {
		ProjectionQueryCriteriaSupport<C, ?> derivedQueryToUse;
		// following code will manage both Sort as an argument, and Sort in a Pageable because getSort() handle both
		if (invocationParameters.getSort().isSorted()) {
			Class<?> declaringClass = method.getEntityInformation().getJavaType();
			// Spring Sort class supports only first-level properties, in-depth ones seems not to be definable,
			// therefore we create AccessorChain of only one property
			ProjectionQueryPageSupport<C> dynamicSortSupport = new ProjectionQueryPageSupport<>();
			invocationParameters.getSort().stream().forEachOrdered(order -> {
				AccessorByMember<?, ?, ?> accessor = Accessors.accessor(declaringClass, order.getProperty());
				dynamicSortSupport.orderBy(new AccessorChain<>(accessor),
						order.getDirection() == Direction.ASC ? Order.ASC : Order.DESC,
						order.isIgnoreCase());
			});
			derivedQueryToUse = actualProjectionQueryCriteriaSupport.copyFor(dynamicSortSupport);
		} else {
			derivedQueryToUse = actualProjectionQueryCriteriaSupport;
		}
		ExecutableProjectionQuery<C, ?> result = derivedQueryToUse.wrapIntoExecutable();
		Limit limit = invocationParameters.getLimit();
		if (limit != null) {
			result.limit(limit.getCount(), limit.getOffset());
		}
		return result;
	}
	
	private static class TupleAccumulator<C> implements Accumulator<Function<Selectable<Object>, Object>, List<Map<String, Object>>, List<Map<String, Object>>> {
		private final IdentityHashMap<JoinLink<?, ?>, AccessorChain<C, ?>> columnToProperties;
		
		public TupleAccumulator(IdentityHashMap<JoinLink<?, ?>, AccessorChain<C, ?>> columnToProperties) {
			this.columnToProperties = columnToProperties;
		}
		
		@Override
		public Supplier<List<Map<String, Object>>> supplier() {
			return LinkedList::new;
		}
		
		@Override
		public BiConsumer<List<Map<String, Object>>, Function<Selectable<Object>, Object>> aggregator() {
			return (finalResult, databaseRowDataProvider) -> {
				Map<String, Object> row = new HashMap<>();
				finalResult.add(row);
				for (Entry<JoinLink<?, ?>, AccessorChain<C, ?>> entry : columnToProperties.entrySet()) {
					PartTreeStalactiteProjection.buildHierarchicMap(entry.getValue(), databaseRowDataProvider.apply((Selectable<Object>) entry.getKey()), row);
				}
			};
		}
		
		@Override
		public Function<List<Map<String, Object>>, List<Map<String, Object>>> finisher() {
			return Function.identity();
		}
	}
}
