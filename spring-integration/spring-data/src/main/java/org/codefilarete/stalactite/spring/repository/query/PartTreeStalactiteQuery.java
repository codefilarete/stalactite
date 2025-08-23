package org.codefilarete.stalactite.spring.repository.query;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.codefilarete.reflection.AccessorByMember;
import org.codefilarete.reflection.AccessorChain;
import org.codefilarete.reflection.Accessors;
import org.codefilarete.stalactite.engine.EntityPersister.OrderByChain.Order;
import org.codefilarete.stalactite.engine.runtime.AdvancedEntityPersister;
import org.codefilarete.stalactite.engine.runtime.query.EntityCriteriaSupport;
import org.codefilarete.stalactite.engine.runtime.query.EntityQueryCriteriaSupport;
import org.codefilarete.stalactite.engine.runtime.query.EntityQueryCriteriaSupport.EntityQueryPageSupport;
import org.codefilarete.stalactite.query.model.LogicalOperator;
import org.codefilarete.stalactite.spring.repository.query.reduce.QueryResultCollectioner;
import org.codefilarete.stalactite.spring.repository.query.reduce.QueryResultPager;
import org.codefilarete.stalactite.spring.repository.query.reduce.QueryResultReducer;
import org.codefilarete.stalactite.spring.repository.query.reduce.QueryResultSingler;
import org.codefilarete.stalactite.spring.repository.query.reduce.QueryResultSlicer;
import org.codefilarete.stalactite.sql.result.Accumulators;
import org.springframework.data.domain.Sort.Direction;
import org.springframework.data.mapping.PropertyPath;
import org.springframework.data.repository.query.ParametersParameterAccessor;
import org.springframework.data.repository.query.QueryMethod;
import org.springframework.data.repository.query.RepositoryQuery;
import org.springframework.data.repository.query.parser.Part;
import org.springframework.data.repository.query.parser.Part.IgnoreCaseType;
import org.springframework.data.repository.query.parser.PartTree;
import org.springframework.data.repository.query.parser.PartTree.OrPart;
import org.springframework.lang.Nullable;

import static org.codefilarete.tool.Nullable.nullable;

/**
 * {@link RepositoryQuery} for Stalactite. Inspired by {@link org.springframework.data.jpa.repository.query.PartTreeJpaQuery}.
 * The parsing of the {@link QueryMethod} is made by Spring {@link PartTree}, hence this class only iterates over parts
 * to create a Stalactite query.
 * 
 * @param <C> entity type
 * @author Guillaume Mary
 */
public class PartTreeStalactiteQuery<C, R> implements StalactiteLimitRepositoryQuery<C, R> {
	
	private final QueryMethod method;
	protected final DerivedQuery<C> query;
	private final AdvancedEntityPersister<C, ?> entityPersister;
	private final PartTree tree;

	public PartTreeStalactiteQuery(QueryMethod method,
								   AdvancedEntityPersister<C, ?> entityPersister,
								   PartTree tree) {
		this.method = method;
		this.entityPersister = entityPersister;
		this.tree = tree;
		
		try {
			this.query = new DerivedQuery<>(entityPersister, tree);
			// Applying sort if necessary
			if (tree.getSort().isSorted()) {
				tree.getSort().iterator().forEachRemaining(order -> {
					PropertyPath propertyPath = PropertyPath.from(order.getProperty(), entityPersister.getClassToPersist());
					AccessorChain<C, Object> orderProperty = query.convertToAccessorChain(propertyPath);
					query.executableEntityQuery.getQueryPageSupport()
							.orderBy(orderProperty, order.getDirection() == Direction.ASC ? Order.ASC : Order.DESC, order.isIgnoreCase());
				});
			}
			// Applying limit if necessary
			nullable(tree.getMaxResults()).invoke(query.executableEntityQuery.getQueryPageSupport()::limit);
		} catch (RuntimeException o_O) {
			throw new IllegalArgumentException(
					String.format("Failed to create query for method %s! %s", method, o_O.getMessage()), o_O);
		}
	}
	
	@Override
	@Nullable
	public R execute(Object[] parameters) {
		query.criteriaChain.consume(parameters);
		
		R adaptation = buildResultWindower().adapt(() -> handleDynamicSort(parameters).<List<C>>wrapGraphLoad(new HashMap<>()).apply(Accumulators.toList())).apply(parameters);
		
		// - hasDynamicProjection() is for case of method that gives the expected returned type as a last argument (or a compound one by Collection or other)
		if (method.getParameters().hasDynamicProjection()) {
			return method.getResultProcessor().withDynamicProjection(new ParametersParameterAccessor(method.getParameters(), parameters)).processResult(adaptation);
		} else {
			return method.getResultProcessor().processResult(adaptation);
		}
	}

	private QueryResultReducer<R, C> buildResultWindower() {
		QueryResultReducer<?, C> result;
		if (method.isPageQuery()) {
			result = new QueryResultPager<>(this, new PartTreeStalactiteCountProjection<>(method, entityPersister, tree));
		} else if (method.isSliceQuery()) {
			result = new QueryResultSlicer<>(this);
		} else if (method.isCollectionQuery()) {
			result = new QueryResultCollectioner<>();
		} else {
			result = new QueryResultSingler<>();
		}
		return (QueryResultReducer<R, C>) result;
	}
	
	private EntityQueryCriteriaSupport<C, ?> handleDynamicSort(Object[] parameters) {
		EntityQueryCriteriaSupport<C, ?> derivedQueryToUse;
		ParametersParameterAccessor parameterHelper = new ParametersParameterAccessor(getQueryMethod().getParameters(), parameters);
		// following code will manage both Sort as an argument, and Sort in a Pageable because getSort() handle both
		if (parameterHelper.getSort().isSorted()) {
			DerivedQuery<C> derivedQuery = new DerivedQuery<>(query.executableEntityQuery);
			Class<?> declaringClass = getQueryMethod().getEntityInformation().getJavaType();
			// Spring Sort class supports only first-level properties, in-depth ones seems not to be definable,
			// therefore we create AccessorChain of only one property 
			parameterHelper.getSort().stream().forEachOrdered(order -> {
				AccessorByMember<?, ?, ?> accessor = Accessors.accessor(declaringClass, order.getProperty());
				derivedQuery.dynamicSortSupport.orderBy(new AccessorChain<>(accessor), order.getDirection() == Direction.ASC ? Order.ASC : Order.DESC, order.isIgnoreCase());
			});
			derivedQueryToUse = derivedQuery.executableEntityQuery.copyFor(derivedQuery.dynamicSortSupport);
		} else {
			derivedQueryToUse = query.executableEntityQuery;
		}
		return derivedQueryToUse;
	}
	
	@Override
	public QueryMethod getQueryMethod() {
		return method;
	}
	
	@Override
	public void limit(int count) {
		this.query.executableEntityQuery.getQueryPageSupport().limit(count);
	}
	
	@Override
	public void limit(int count, Integer offset) {
		this.query.executableEntityQuery.getQueryPageSupport().limit(count, offset);
	}
	
	static class DerivedQuery<T> extends AbstractDerivedQuery<T> {
		
		protected final EntityQueryCriteriaSupport<T, ?> executableEntityQuery;
		
		protected final EntityQueryPageSupport<T> dynamicSortSupport = new EntityQueryPageSupport<>();
		
		protected EntityCriteriaSupport<T> currentSupport;
		
		private DerivedQuery(AdvancedEntityPersister<T, ?> entityPersister, PartTree tree) {
			this.executableEntityQuery = entityPersister.newCriteriaSupport();
			tree.forEach(this::append);
		}
		
		private DerivedQuery(EntityQueryCriteriaSupport<T, ?> executableEntityQuery) {
			this.executableEntityQuery = executableEntityQuery;
		}
		
		private void append(OrPart part) {
			this.currentSupport = this.executableEntityQuery.getEntityCriteriaSupport();
			boolean nested = false;
			if (part.stream().count() > 1) {	// "if" made to avoid extra parenthesis (can be considered superfluous)
				nested = true;
				this.currentSupport = currentSupport.beginNested();
			}
			Iterator<Part> iterator = part.iterator();
			if (iterator.hasNext()) {
				append(iterator.next(), LogicalOperator.OR);
			}
			iterator.forEachRemaining(p -> this.append(p, LogicalOperator.AND));
			if (nested) {	// "if" made to avoid extra parenthesis (can be considered superfluous)
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
