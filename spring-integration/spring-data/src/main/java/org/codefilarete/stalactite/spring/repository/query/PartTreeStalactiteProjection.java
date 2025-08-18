package org.codefilarete.stalactite.spring.repository.query;

import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.codefilarete.reflection.AccessorByMember;
import org.codefilarete.reflection.AccessorChain;
import org.codefilarete.reflection.Accessors;
import org.codefilarete.reflection.ValueAccessPoint;
import org.codefilarete.stalactite.engine.EntityPersister.OrderByChain.Order;
import org.codefilarete.stalactite.engine.runtime.AdvancedEntityPersister;
import org.codefilarete.stalactite.engine.runtime.ProjectionQueryCriteriaSupport;
import org.codefilarete.stalactite.engine.runtime.ProjectionQueryCriteriaSupport.ProjectionQueryPageSupport;
import org.codefilarete.stalactite.engine.runtime.query.AggregateAccessPointToColumnMapping;
import org.codefilarete.stalactite.engine.runtime.query.EntityCriteriaSupport;
import org.codefilarete.stalactite.query.model.LogicalOperator;
import org.codefilarete.stalactite.query.model.Select;
import org.codefilarete.stalactite.query.model.Selectable;
import org.codefilarete.stalactite.sql.result.Accumulator;
import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.tool.collection.KeepOrderSet;
import org.codefilarete.tool.function.Hanger.Holder;
import org.codefilarete.tool.trace.MutableInt;
import org.springframework.data.domain.Sort.Direction;
import org.springframework.data.mapping.PropertyPath;
import org.springframework.data.projection.EntityProjection;
import org.springframework.data.projection.EntityProjectionIntrospector;
import org.springframework.data.projection.EntityProjectionIntrospector.ProjectionPredicate;
import org.springframework.data.projection.ProjectionFactory;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
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
class PartTreeStalactiteProjection<C, R> implements StalactiteLimitRepositoryQuery<C, R> {
	
	private final QueryMethod method;
	private final AdvancedEntityPersister<C, ?> entityPersister;
	private final PartTree tree;
	private final Accumulator<Function<Selectable<Object>, Object>, List<Object[]>, List<Object[]>> accumulator;
	private final DerivedQuery<C> query;
	private final Consumer<Select> selectConsumer;
	private Function<List<Object[]>, R> rowProjectionAdapter;
	
	public <O> PartTreeStalactiteProjection(
			QueryMethod method,
			AdvancedEntityPersister<C, ?> entityPersister,
			PartTree tree,
			ProjectionFactory factory) {
		this.method = method;
		this.entityPersister = entityPersister;
		this.tree = tree;
		
		// The projection is closed: it means there's not @Value on the interface, so we can use Spring property introspector to look up for
		// properties to select in the query
		// If the projection is open (any method as a @Value on it), then, because Spring can't know in advance which field will be required to
		// evaluate the @Value expression, we must retrieve the whole aggregate as entities.
		// se https://docs.spring.io/spring-data/jpa/reference/repositories/projections.html
		ProjectionPredicate predicate = (returnType, domainType)
				-> !domainType.isAssignableFrom(returnType) && !returnType.isAssignableFrom(domainType);
		
		EntityProjectionIntrospector entityProjectionIntrospector = EntityProjectionIntrospector.create(factory, predicate, new RelationalMappingContext());
		EntityProjection<?, C> introspect = entityProjectionIntrospector.introspect(method.getReturnedObjectType(), entityPersister.getClassToPersist());
		Set<List<ValueAccessPoint<?>>> propertiesAccessors = new LinkedHashSet<>();
		// there's a bug here: when property length is higher than 2 it contains an extra item which makes the whole algorithm broken (Spring bug)
		introspect.forEachRecursive(propertyProjection -> {
			AccessorChain accessorChain = new AccessorChain<>();
			List<PropertyPath> collect = propertyProjection.getPropertyPath().stream().collect(Collectors.toList());
			if (collect.size() >= 2) {
				collect = Iterables.cutTail(collect);
			}
			collect.forEach(propertyPath -> {
				propertyPath.forEach(propertyPath1 -> {
					accessorChain.add(Accessors.accessor(propertyPath1.getOwningType().getType(), propertyPath1.getSegment(), propertyPath1.getType()));
				});
			});
			propertiesAccessors.add(accessorChain.getAccessors());
		});
		
		AggregateAccessPointToColumnMapping<C> aggregateColumnMapping = entityPersister.getEntityFinder().newCriteriaSupport().getEntityCriteriaSupport().getAggregateColumnMapping();
		Holder<Select> selectHolder = new Holder<>();
		this.selectConsumer = select -> {
			select.clear();
			selectHolder.set(select);
			MutableInt aliasCounter = new MutableInt(0);
			propertiesAccessors.forEach(property -> {
				Selectable<?> selectable = aggregateColumnMapping.giveColumn(property);
				select.add(selectable, "prop_" + aliasCounter.increment());
			});
		};
		
		// Note that for ResultProcessor need (at execution time), we must provide each row as either Object[] or Collection
		// (see ResultProcessor.ProjectingConverter.getProjectionTarget)
		// we chose to use Object[] for clarity: there are already many Collections in here, then using Object[] clarifies a bit for which usage we
		// type the things
		this.accumulator = new Accumulator<Function<Selectable<Object>, Object>, List<Object[]>, List<Object[]>>() {
			@Override
			public Supplier<List<Object[]>> supplier() {
				return LinkedList::new;
			}
			
			@Override
			public BiConsumer<List<Object[]>, Function<Selectable<Object>, Object>> aggregator() {
				return (resultSet, selectableObjectFunction) -> {
					Select selectables = selectHolder.get();
					KeepOrderSet<Selectable<?>> columns = selectables.getColumns();
					Object[] row = new Object[columns.size()];
					resultSet.add(row);
					int i = 0;
					for (Selectable<?> selectable : columns) {
						row[i++] = selectableObjectFunction.apply((Selectable<Object>) selectable);
					}
				};
			}
			
			@Override
			public Function<List<Object[]>, List<Object[]>> finisher() {
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
		
		ProjectionQueryCriteriaSupport<C, ?> derivedQueryToUse = handleDynamicSort(parameters);
		
		QueryResultWindower<C, ?, R, Object[]> queryResultWindower;
		Supplier<List<Object[]>> resultSupplier = () -> derivedQueryToUse.wrapIntoExecutable().execute(accumulator);
		
		if (method.isSliceQuery()) {
			queryResultWindower = new SliceResultWindower<>(this, resultSupplier);
		} else if (method.isPageQuery()) {
			queryResultWindower = new PageResultWindower<>(this, new PartTreeStalactiteCountProjection<>(method, entityPersister, tree), resultSupplier);
		} else {
			// the result type might be a Collection or a single result
			queryResultWindower = new QueryResultWindower<C, Object, R, Object[]>(null, null, null) {
				@Override
				R adaptExecution(Object[] parameters) {
					return (R) derivedQueryToUse.wrapIntoExecutable().execute(accumulator);
				}
			};
		}
		
		R adaptation = queryResultWindower.adaptExecution(parameters);
		
		// - hasDynamicProjection() is for case of method that gives the expected returned type as a last argument (or a compound one by Collection or other)
		if (method.getParameters().hasDynamicProjection()) {
			return method.getResultProcessor().withDynamicProjection(new ParametersParameterAccessor(method.getParameters(), parameters)).processResult(adaptation);
		} else {
			return method.getResultProcessor().processResult(adaptation);
		}
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
