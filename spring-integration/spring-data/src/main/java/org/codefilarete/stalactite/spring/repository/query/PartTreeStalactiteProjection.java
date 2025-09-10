package org.codefilarete.stalactite.spring.repository.query;

import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import org.codefilarete.reflection.Accessor;
import org.codefilarete.reflection.AccessorByMember;
import org.codefilarete.reflection.AccessorChain;
import org.codefilarete.reflection.AccessorDefinition;
import org.codefilarete.reflection.Accessors;
import org.codefilarete.stalactite.engine.EntityPersister.ExecutableProjectionQuery;
import org.codefilarete.stalactite.engine.EntityPersister.OrderByChain.Order;
import org.codefilarete.stalactite.engine.runtime.AdvancedEntityPersister;
import org.codefilarete.stalactite.engine.runtime.ProjectionQueryCriteriaSupport;
import org.codefilarete.stalactite.engine.runtime.ProjectionQueryCriteriaSupport.ProjectionQueryPageSupport;
import org.codefilarete.stalactite.engine.runtime.RelationalEntityPersister;
import org.codefilarete.stalactite.engine.runtime.query.EntityQueryCriteriaSupport;
import org.codefilarete.stalactite.engine.runtime.query.EntityQueryCriteriaSupport.EntityQueryPageSupport;
import org.codefilarete.stalactite.query.model.JoinLink;
import org.codefilarete.stalactite.query.model.Limit;
import org.codefilarete.stalactite.query.model.Selectable;
import org.codefilarete.stalactite.spring.repository.query.reduce.LimitHandler;
import org.codefilarete.stalactite.spring.repository.query.reduce.QueryResultCollectioner;
import org.codefilarete.stalactite.spring.repository.query.reduce.QueryResultPager;
import org.codefilarete.stalactite.spring.repository.query.reduce.QueryResultReducer;
import org.codefilarete.stalactite.spring.repository.query.reduce.QueryResultSingler;
import org.codefilarete.stalactite.spring.repository.query.reduce.QueryResultSlicer;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.result.Accumulator;
import org.codefilarete.stalactite.sql.result.Accumulators;
import org.codefilarete.tool.VisibleForTesting;
import org.springframework.data.domain.Sort.Direction;
import org.springframework.data.projection.ProjectionFactory;
import org.springframework.data.repository.query.Parameter;
import org.springframework.data.repository.query.parser.PartTree;

public class PartTreeStalactiteProjection<C, R> extends AbstractRepositoryQuery<C, R> {
	
	/**
	 * Fills given {@link Map} with some more {@link Map}s to create a hierarchic structure from the given dotted property name, e.g. "a.b.c" will
	 * result in a map like:
	 * <pre>{@code
	 *   "a": {
	 *     "b": {
	 *       "c": value
	 *     }
	 *   }
	 * }</pre>
	 * If the given Map already contains data, then it will be filled without overriding the existing ones, e.g. given the above {@link Map}, if we
	 * call this method with "a.b.d" and a value, then the resulting {@link Map} will be:
	 * <pre>{@code
	 *   "a": {
	 *     "b": {
	 *       "c": value
	 *       "d": other_value
	 *     }
	 *   }
	 * }</pre>	 *
	 *
	 * @param dottedProperty the dotted property name
	 * @param value the value to set at the leaf of the map
	 * @param root the root map to build upon
	 */
	@VisibleForTesting
	public static void buildHierarchicMap(AccessorChain<?, ?> dottedProperty, Object value, Map<String, Object> root) {
		Map<String, Object> current = root;
		// Navigate through all parts except the last one
		int lengthMinus1 = dottedProperty.getAccessors().size() - 1;
		for (int i = 0; i < lengthMinus1; i++) {
			Accessor<?, ?> accessor = dottedProperty.getAccessors().get(i);
			String propertyName = AccessorDefinition.giveDefinition(accessor).getName();
			current = (Map<String, Object>) current.computeIfAbsent(propertyName, k -> new HashMap<>());
		}
		// Set the value in the leaf Map
		Accessor<?, ?> lastAccessor = dottedProperty.getAccessors().get(lengthMinus1);
		String propertyName = AccessorDefinition.giveDefinition(lastAccessor).getName();
		current.putIfAbsent(propertyName, value);
	}
	
	private final ProjectionMappingFinder<C> projectionMappingFinder;
	private final AdvancedEntityPersister<C, ?> entityPersister;
	private final PartTree partTree;
	private final ProjectionFactory factory;
	private final Dialect dialect;
	private final PartTreeStalactiteCountProjection<C> countQuery;
	private final ProjectionQueryCriteriaSupport<C, ?> projectionQueryCriteriaSupport;
	
	public PartTreeStalactiteProjection(StalactiteQueryMethod method,
										AdvancedEntityPersister<C, ?> entityPersister,
										PartTree partTree,
										ProjectionFactory factory,
										Dialect dialect) {
		super(method);
		this.entityPersister = entityPersister;
		this.partTree = partTree;
		this.factory = factory;
		this.dialect = dialect;
		
		this.countQuery = new PartTreeStalactiteCountProjection<>(method, entityPersister, partTree);
		this.projectionMappingFinder = new ProjectionMappingFinder<>(factory, entityPersister);
		
		this.projectionMappingFinder.lookup(method.getDomainClass());
		// by default, we don't customize the select clause because it will be adapted at very last time, during execution according to the projection
		// type which can be dynamic
		this.projectionQueryCriteriaSupport = entityPersister.newProjectionCriteriaSupport(selectables -> {});
		ToCriteriaPartTreeTransformer<C> projectionCriteriaAppender = new ToCriteriaPartTreeTransformer<>(partTree, entityPersister.getClassToPersist(), projectionQueryCriteriaSupport);
	}
	
	@Override
	protected AbstractQueryExecutor<List<Object>, Object> buildQueryExecutor(StalactiteQueryMethodInvocationParameters invocationParameters) {
		// Extracting the Selectable and PropertyPath from the projection type
		boolean runProjectionQuery;
		IdentityHashMap<JoinLink<?, ?>, AccessorChain<C, ?>> propertiesColumns;
		if (method.getParameters().hasDynamicProjection()) {
			propertiesColumns = this.projectionMappingFinder.lookup(invocationParameters.getDynamicProjectionType());
			runProjectionQuery = factory.getProjectionInformation(invocationParameters.getDynamicProjectionType()).isClosed()
					&& !invocationParameters.getDynamicProjectionType().isAssignableFrom(entityPersister.getClassToPersist());
		} else {
			propertiesColumns = this.projectionMappingFinder.lookup(method.getReturnedObjectType());
			runProjectionQuery = factory.getProjectionInformation(method.getReturnedObjectType()).isClosed();
		}
		if (runProjectionQuery) {
			return createClosedProjectionExecutor(invocationParameters, propertiesColumns);
		} else {
			// if the projection is not closed (contains @Value for example), then we must fetch the whole entity
			// because we can't know in advance which property will be required to evaluate the @Value
			// therefore we use the default query that select all columns of the aggregate
			// see https://docs.spring.io/spring-data/jpa/reference/repositories/projections.html
			return (AbstractQueryExecutor) createDomainQueryExecutor();
		}
	}
	
	private AbstractQueryExecutor<List<C>, C> createDomainQueryExecutor() {
		return new AbstractQueryExecutor<List<C>, C>(method, dialect) {
			
			@Override
			public Supplier<List<C>> buildQueryExecutor(Object[] parameters) {
				EntityQueryCriteriaSupport<C, ?> executableEntityQuery = entityPersister.newCriteriaSupport();
				ToCriteriaPartTreeTransformer<C> criteriaAppender = new ToCriteriaPartTreeTransformer<>(
						partTree,
						entityPersister.getClassToPersist(),
						executableEntityQuery.getEntityCriteriaSupport(),
						executableEntityQuery.getQueryPageSupport(),
						executableEntityQuery.getQueryPageSupport());
				criteriaAppender.condition.consume(parameters);
				
				R adaptation = buildResultWindower(executableEntityQuery).adapt(() -> {
					StalactiteQueryMethodInvocationParameters invocationParameters = new StalactiteQueryMethodInvocationParameters(method, parameters);
					RelationalEntityPersister.ExecutableEntityQueryCriteria<C, ?> executableEntityQueryCriteria = handleDynamicSort(invocationParameters, executableEntityQuery).wrapIntoExecutable();
					int i = 1;
					for (Parameter bindableParameter : invocationParameters.getParameters().getBindableParameters()) {
						Object value = invocationParameters.getBindableValue(bindableParameter.getIndex());
						// Note that we don't apply the value to the criteria of the derived query template, else we temper with it for next execution
						// defaultDerivedQuery.criteriaChain.criteria.get(i).setValue(..);
						executableEntityQueryCriteria.set(String.valueOf(i++), value);
					}
					return executableEntityQueryCriteria.execute(Accumulators.toList());
				}).apply(parameters);
				
				return () -> method.getResultProcessor().processResult(adaptation);
			}
		};
	}
	
	private EntityQueryCriteriaSupport<C, ?> handleDynamicSort(StalactiteQueryMethodInvocationParameters invocationParameters,
															   EntityQueryCriteriaSupport<C, ?> defaultExecutableEntityQuery) {
		EntityQueryCriteriaSupport<C, ?> derivedQueryToUse;
		// following code will manage both Sort as an argument, and Sort in a Pageable because getSort() handle both
		if (invocationParameters.getSort().isSorted()) {
			Class<?> declaringClass = getQueryMethod().getEntityInformation().getJavaType();
			// Spring Sort class supports only first-level properties, in-depth ones seems not to be definable,
			// therefore we create AccessorChain of only one property
			EntityQueryPageSupport<C> dynamicSortSupport = new EntityQueryPageSupport<>();
			invocationParameters.getSort().stream().forEachOrdered(order -> {
				AccessorByMember<?, ?, ?> accessor = Accessors.accessor(declaringClass, order.getProperty());
				dynamicSortSupport.orderBy(new AccessorChain<>(accessor),
						order.getDirection() == Direction.ASC ? Order.ASC : Order.DESC,
						order.isIgnoreCase());
			});
			derivedQueryToUse = defaultExecutableEntityQuery.copyFor(dynamicSortSupport);
		} else {
			derivedQueryToUse = defaultExecutableEntityQuery;
		}
		return derivedQueryToUse;
	}
	
	private QueryResultReducer<R, C> buildResultWindower(EntityQueryCriteriaSupport<C, ?> executableEntityQuery) {
		QueryResultReducer<?, C> result;
		if (method.isPageQuery()) {
			result = new QueryResultPager<>(this, new LimitHandler() {
				@Override
				public void limit(int count) {
					executableEntityQuery.getQueryPageSupport().limit(count);
				}
				
				@Override
				public void limit(int count, Integer offset) {
					executableEntityQuery.getQueryPageSupport().limit(count, offset);
				}
			}, new PartTreeStalactiteCountProjection<>(method, entityPersister, partTree));
		} else if (method.isSliceQuery()) {
			result = new QueryResultSlicer<>(this, new LimitHandler() {
				@Override
				public void limit(int count) {
					executableEntityQuery.getQueryPageSupport().limit(count);
				}
				
				@Override
				public void limit(int count, Integer offset) {
					executableEntityQuery.getQueryPageSupport().limit(count, offset);
				}
			});
		} else if (method.isCollectionQuery()) {
			result = new QueryResultCollectioner<>();
		} else {
			result = new QueryResultSingler<>();
		}
		return (QueryResultReducer<R, C>) result;
	}
	
	private AbstractQueryExecutor<List<Object>, Object> createClosedProjectionExecutor(
			StalactiteQueryMethodInvocationParameters invocationParameters,
			IdentityHashMap<JoinLink<?, ?>, AccessorChain<C, ?>> columnToProperties) {
		IdentityHashMap<JoinLink<?, ?>, String> aliases = buildAliases(columnToProperties);
		
		ProjectionQueryCriteriaSupport<C, ?> actualProjectionQueryCriteriaSupport = projectionQueryCriteriaSupport.copyFor(select -> {
			select.clear();
			columnToProperties.keySet().forEach(selectable -> {
				select.add(selectable, aliases.get(selectable));
			});
		});
		Accumulator<Function<Selectable<Object>, Object>, List<Map<String, Object>>, List<Map<String, Object>>> accumulator = new Accumulator<Function<Selectable<Object>, Object>, List<Map<String, Object>>, List<Map<String, Object>>>() {
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
						buildHierarchicMap(entry.getValue(), databaseRowDataProvider.apply((Selectable<Object>) entry.getKey()), row);
					}
				};
			}
			
			@Override
			public Function<List<Map<String, Object>>, List<Map<String, Object>>> finisher() {
				return Function.identity();
			}
		};
		
		return new AbstractQueryExecutor<List<Object>, Object>(method, dialect) {
			@Override
			public Supplier<List<Object>> buildQueryExecutor(Object[] parameters) {
				return () -> {
					// TODO: remove this usage everywhere else, to be replaced by the bindable parameters loop below
//					query.criteriaChain.consume(parameters);
					ExecutableProjectionQuery<C, ?> projectionQuery = handleDynamicParameters(invocationParameters, actualProjectionQueryCriteriaSupport);
					
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
		};
	}
	
	private ExecutableProjectionQuery<C, ?> handleDynamicParameters(StalactiteQueryMethodInvocationParameters invocationParameters,
																   ProjectionQueryCriteriaSupport<C, ?> actualProjectionQueryCriteriaSupport) {
		ProjectionQueryCriteriaSupport<C, ?> derivedQueryToUse;
		// following code will manage both Sort as an argument, and Sort in a Pageable because getSort() handle both
		if (invocationParameters.getSort().isSorted()) {
			Class<?> declaringClass = getQueryMethod().getEntityInformation().getJavaType();
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
	
	@Override
	protected LongSupplier buildCountSupplier(StalactiteQueryMethodInvocationParameters accessor) {
		return () -> countQuery.execute(accessor.getValues());
	}
}
