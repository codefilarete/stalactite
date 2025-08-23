package org.codefilarete.stalactite.spring.repository.query;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import org.codefilarete.reflection.AccessorByMember;
import org.codefilarete.reflection.AccessorChain;
import org.codefilarete.reflection.Accessors;
import org.codefilarete.stalactite.engine.EntityPersister.OrderByChain.Order;
import org.codefilarete.stalactite.engine.runtime.AdvancedEntityPersister;
import org.codefilarete.stalactite.engine.runtime.ProjectionQueryCriteriaSupport;
import org.codefilarete.stalactite.engine.runtime.ProjectionQueryCriteriaSupport.ProjectionQueryPageSupport;
import org.codefilarete.stalactite.engine.runtime.query.EntityCriteriaSupport;
import org.codefilarete.stalactite.query.model.LogicalOperator;
import org.codefilarete.stalactite.query.model.Select;
import org.codefilarete.stalactite.query.model.Selectable;
import org.codefilarete.stalactite.spring.repository.query.reduce.QueryResultCollectioner;
import org.codefilarete.stalactite.spring.repository.query.reduce.QueryResultPager;
import org.codefilarete.stalactite.spring.repository.query.reduce.QueryResultReducer;
import org.codefilarete.stalactite.spring.repository.query.reduce.QueryResultSingler;
import org.codefilarete.stalactite.spring.repository.query.reduce.QueryResultSlicer;
import org.codefilarete.stalactite.sql.result.Accumulator;
import org.codefilarete.tool.VisibleForTesting;
import org.codefilarete.tool.collection.KeepOrderSet;
import org.codefilarete.tool.function.Hanger.Holder;
import org.springframework.data.domain.Sort.Direction;
import org.springframework.data.mapping.PropertyPath;
import org.springframework.data.projection.ProjectionFactory;
import org.springframework.data.repository.query.ParametersParameterAccessor;
import org.springframework.data.repository.query.QueryMethod;
import org.springframework.data.repository.query.RepositoryQuery;
import org.springframework.data.repository.query.parser.Part;
import org.springframework.data.repository.query.parser.Part.IgnoreCaseType;
import org.springframework.data.repository.query.parser.PartTree;
import org.springframework.data.repository.query.parser.PartTree.OrPart;

import static org.codefilarete.tool.Nullable.nullable;

/**
 * {@link RepositoryQuery} for Stalactite count order.
 *
 * @param <C> entity type
 * @author Guillaume Mary
 */
public class PartTreeStalactiteProjection<C, R> implements StalactiteLimitRepositoryQuery<C, R> {
	
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
	 * @param dottedProperty the dotted property name
	 * @param value the value to set at the leaf of the map
	 * @param root the root map to build upon
	 */
	@VisibleForTesting
	public static void buildHierarchicMap(String dottedProperty, Object value, Map<String, Object> root) {
		String[] parts = dottedProperty.split("\\.");
		Map<String, Object> current = root;
		// Navigate through all parts except the last one
		int lengthMinus1 = parts.length - 1;
		for (int i = 0; i < lengthMinus1; i++) {
			current = (Map<String, Object>) current.computeIfAbsent(parts[i], k -> new HashMap<>());
		}
		// Set the value in the leaf Map
		current.putIfAbsent(parts[lengthMinus1], value);
	}

	private final QueryMethod method;
	private final AdvancedEntityPersister<C, ?> entityPersister;
	private final PartTree tree;
	private final Accumulator<Function<Selectable<Object>, Object>, List<Map<String, Object>>, List<Map<String, Object>>> accumulator;
	private final DerivedQuery<C> query;
	private final Consumer<Select> selectConsumer;
	
	public PartTreeStalactiteProjection(
			QueryMethod method,
			AdvancedEntityPersister<C, ?> entityPersister,
			PartTree tree,
			ProjectionFactory factory) {
		this.method = method;
		this.entityPersister = entityPersister;
		this.tree = tree;
		
		ProjectionTypeInformationExtractor<C> projectionTypeInformationExtractor = new ProjectionTypeInformationExtractor<>(factory, entityPersister);
		// Extracting the Selectable and PropertyPath from the aggregate
		projectionTypeInformationExtractor.extract(method.getReturnedObjectType());
		
		Holder<Select> selectHolder = new Holder<>();
		this.selectConsumer = select -> {
			select.clear();
			selectHolder.set(select);
			projectionTypeInformationExtractor.getColumnToProperties().keySet().forEach(selectable -> {
				select.add(selectable, projectionTypeInformationExtractor.getAliases().get(selectable));
			});
		};
		
		// Note that, to suit Spring Data unmarshalling (in ResultProcessor), we must provide each row as a Map<String, Map<String, Map<String, ...>>>
		// to mimic a hierarchic structure
		this.accumulator = new Accumulator<Function<Selectable<Object>, Object>, List<Map<String, Object>>, List<Map<String, Object>>>() {
			@Override
			public Supplier<List<Map<String, Object>>> supplier() {
				return LinkedList::new;
			}
			
			@Override
			public BiConsumer<List<Map<String, Object>>, Function<Selectable<Object>, Object>> aggregator() {
				return (finalResult, databaseRowDataProvider) -> {
					Select selectables = selectHolder.get();
					KeepOrderSet<Selectable<?>> columns = selectables.getColumns();
					Map<String, Object> row = new HashMap<>();
					finalResult.add(row);
					for (Selectable<?> selectable : columns) {
						buildHierarchicMap(
								projectionTypeInformationExtractor.getColumnToProperties().get(selectable).toDotPath(),
								databaseRowDataProvider.apply((Selectable<Object>) selectable),
								row
						);
					}
				};
			}
			
			@Override
			public Function<List<Map<String, Object>>, List<Map<String, Object>>> finisher() {
				return Function.identity();
			}
		};
		
		try {
			this.query = new DerivedQuery<>(entityPersister, tree);
			// Applying sort if necessary
			if (tree.getSort().isSorted()) {
				tree.getSort().iterator().forEachRemaining(order -> {
					PropertyPath propertyPath = PropertyPath.from(order.getProperty(), entityPersister.getClassToPersist());
					AccessorChain<C, Object> orderProperty = query.convertToAccessorChain(propertyPath);
					query.executableProjectionQuery.getQueryPageSupport()
							.orderBy(orderProperty, order.getDirection() == Direction.ASC ? Order.ASC : Order.DESC, order.isIgnoreCase());
				});
			}
			// Applying limit if necessary
			nullable(tree.getMaxResults()).invoke(query.executableProjectionQuery.getQueryPageSupport()::limit);
		} catch (RuntimeException o_O) {
			throw new IllegalArgumentException(
					String.format("Failed to create query for method %s! %s", method, o_O.getMessage()), o_O);
		}
	}
	
	public DerivedQuery<C> getQuery() {
		return query;
	}
	
	@Override
	public R execute(Object[] parameters) {
		query.criteriaChain.consume(parameters);
		
		Supplier<List<Map<String, Object>>> resultSupplier = buildQueryExecutor(parameters);

		R adaptation = buildResultWindower().adapt(resultSupplier).apply(parameters);
		
		// - hasDynamicProjection() is for case of method that gives the expected returned type as a last argument (or a compound one by Collection or other)
		if (method.getParameters().hasDynamicProjection()) {
			return method.getResultProcessor().withDynamicProjection(new ParametersParameterAccessor(method.getParameters(), parameters)).processResult(adaptation);
		} else {
			return method.getResultProcessor().processResult(adaptation);
		}
	}

	private QueryResultReducer<R, Map<String, Object>> buildResultWindower() {
		QueryResultReducer<?, Map<String, Object>> result;
		if (method.isPageQuery()) {
			result = new QueryResultPager<>(this, new PartTreeStalactiteCountProjection<>(method, entityPersister, tree));
		} else if (method.isSliceQuery()) {
			result = new QueryResultSlicer<>(this);
		} else if (method.isCollectionQuery()) {
			result = new QueryResultCollectioner<>();
		} else {
			result = new QueryResultSingler<>();
		}
		return (QueryResultReducer<R, Map<String, Object>>) result;
	}
	
	protected Supplier<List<Map<String, Object>>> buildQueryExecutor(Object[] parameters) {
		return () -> handleDynamicSort(parameters).wrapIntoExecutable().execute(accumulator);
	}
	
	private ProjectionQueryCriteriaSupport<C, ?> handleDynamicSort(Object[] parameters) {
		ProjectionQueryCriteriaSupport<C, ?> derivedQueryToUse;
		ParametersParameterAccessor parameterHelper = new ParametersParameterAccessor(getQueryMethod().getParameters(), parameters);
		// following code will manage both Sort as an argument, and Sort in a Pageable because getSort() handle both
		if (parameterHelper.getSort().isSorted()) {
			DerivedQuery<C> derivedQuery = new DerivedQuery<>(query.executableProjectionQuery);
			Class<?> declaringClass = getQueryMethod().getEntityInformation().getJavaType();
			// Spring Sort class supports only first-level properties, in-depth ones seems not to be definable,
			// therefore we create AccessorChain of only one property 
			parameterHelper.getSort().stream().forEachOrdered(order -> {
				AccessorByMember<?, ?, ?> accessor = Accessors.accessor(declaringClass, order.getProperty());
				derivedQuery.dynamicSortSupport.orderBy(new AccessorChain<>(accessor),
						order.getDirection() == Direction.ASC ? Order.ASC : Order.DESC,
						order.isIgnoreCase());
			});
			derivedQueryToUse = derivedQuery.executableProjectionQuery.copyFor(derivedQuery.dynamicSortSupport);
		} else {
			derivedQueryToUse = query.executableProjectionQuery;
		}
		return derivedQueryToUse;
	}
	
	@Override
	public QueryMethod getQueryMethod() {
		return method;
	}
	
	@Override
	public void limit(int count) {
		this.query.executableProjectionQuery.getQueryPageSupport().limit(count);
	}
	
	@Override
	public void limit(int count, Integer offset) {
		this.query.executableProjectionQuery.getQueryPageSupport().limit(count, offset);
	}
	
	private class DerivedQuery<T> extends AbstractDerivedQuery<T> {
		
		protected final ProjectionQueryCriteriaSupport<T, ?> executableProjectionQuery;
		
		protected final ProjectionQueryPageSupport<T> dynamicSortSupport = new ProjectionQueryPageSupport<>();
		
		private EntityCriteriaSupport<T> currentSupport;
		
		private DerivedQuery(AdvancedEntityPersister<T, ?> entityPersister, PartTree tree) {
			this.executableProjectionQuery = entityPersister.newProjectionCriteriaSupport(selectConsumer);
			tree.forEach(this::append);
		}
		
		private DerivedQuery(ProjectionQueryCriteriaSupport<T, ?> executableProjectionQuery) {
			this.executableProjectionQuery = executableProjectionQuery;
		}
		
		private void append(OrPart part) {
			this.currentSupport = this.executableProjectionQuery.getEntityCriteriaSupport();
			boolean nested = false;
			if (part.stream().count() > 1) {    // "if" made to avoid extra parenthesis (can be considered superfluous)
				nested = true;
				this.currentSupport = currentSupport.beginNested();
			}
			Iterator<Part> iterator = part.iterator();
			if (iterator.hasNext()) {
				append(iterator.next(), LogicalOperator.OR);
			}
			iterator.forEachRemaining(p -> this.append(p, LogicalOperator.AND));
			if (nested) {    // "if" made to avoid extra parenthesis (can be considered superfluous)
				this.currentSupport = currentSupport.endNested();
			}
		}
		
		private void append(Part part, LogicalOperator orOrAnd) {
			AccessorChain<T, Object> getter = convertToAccessorChain(part.getProperty());
			Criterion criterion = convertToCriterion(part.getType(), part.shouldIgnoreCase() != IgnoreCaseType.NEVER);
			
			this.currentSupport.add(orOrAnd, getter.getAccessors(), criterion.operator);
			super.criteriaChain.criteria.add(criterion);
		}
	}
}
