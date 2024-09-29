package org.codefilarete.stalactite.sql.spring.repository.query;

import java.util.Collection;

import org.codefilarete.reflection.AccessorByMember;
import org.codefilarete.reflection.AccessorChain;
import org.codefilarete.reflection.Accessors;
import org.codefilarete.stalactite.engine.EntityPersister.OrderByChain.Order;
import org.codefilarete.stalactite.engine.runtime.AdvancedEntityPersister;
import org.codefilarete.stalactite.engine.runtime.EntityQueryCriteriaSupport;
import org.codefilarete.stalactite.engine.runtime.EntityQueryCriteriaSupport.EntityQueryPageSupport;
import org.codefilarete.stalactite.sql.result.Accumulator;
import org.springframework.data.domain.Sort.Direction;
import org.springframework.data.jpa.repository.query.PartTreeJpaQuery;
import org.springframework.data.mapping.PropertyPath;
import org.springframework.data.repository.query.ParameterAccessor;
import org.springframework.data.repository.query.Parameters;
import org.springframework.data.repository.query.ParametersParameterAccessor;
import org.springframework.data.repository.query.QueryMethod;
import org.springframework.data.repository.query.RepositoryQuery;
import org.springframework.data.repository.query.parser.Part;
import org.springframework.data.repository.query.parser.PartTree;
import org.springframework.lang.Nullable;

import static org.codefilarete.tool.Nullable.nullable;

/**
 * {@link RepositoryQuery} for Stalactite. Inspired by {@link PartTreeJpaQuery}.
 * The parsing of the {@link QueryMethod} is made by Spring {@link PartTree}, hence this class only iterates over parts
 * to create a Stalactite query.
 * 
 * @param <C> entity type
 * @author Guillaume Mary
 */
public class PartTreeStalactiteQuery<C, R> implements RepositoryQuery {
	
	protected final QueryMethod method;
	protected final DerivedQuery<C> query;
	protected final Accumulator<C, Collection<C>, R> accumulator;
	
	public PartTreeStalactiteQuery(QueryMethod method,
								   AdvancedEntityPersister<C, ?> entityPersister,
								   PartTree tree,
								   Accumulator<C, ? extends Collection<C>, R> accumulator) {
		this.method = method;
		this.accumulator = (Accumulator<C, Collection<C>, R>) accumulator;
		Parameters<?, ?> parameters = method.getParameters();
		
		try {
			this.query = new DerivedQuery<>(entityPersister, tree);
			// Applying sort if necessary
			if (tree.getSort().isSorted()) {
				tree.getSort().iterator().forEachRemaining(order -> {
					PropertyPath propertyPath = PropertyPath.from(order.getProperty(), entityPersister.getClassToPersist());
					AccessorChain<C, Object> orderProperty = query.convertToAccessorChain(propertyPath);
					query.executableEntityQuery.getQueryPageSupport()
							.orderBy(orderProperty, order.getDirection() == Direction.ASC ? Order.ASC : Order.DESC);
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
		
		EntityQueryCriteriaSupport<C, ?> derivedQueryToUse = handleDynamicSort(parameters);
		R result = derivedQueryToUse.<R>wrapGraphLoad().apply(accumulator);
		
		// - isProjecting() is for case of return type is not domain one (nor a compound one by Collection or other)
		// - hasDynamicProjection() is for case of method that gives the expected returned type as a last argument (or a compound one by Collection or other)
		if (method.getResultProcessor().getReturnedType().isProjecting() || method.getParameters().hasDynamicProjection()) {
			ParameterAccessor accessor = new ParametersParameterAccessor(method.getParameters(), parameters);
			// withDynamicProjection() handles the 2 cases of the "if" (with some not obvious algorithm)
			return method.getResultProcessor().withDynamicProjection(accessor).processResult(result);
		} else {
			return result;
		}
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
				derivedQuery.dynamicSortSupport.orderBy(new AccessorChain<>(accessor), order.getDirection() == Direction.ASC ? Order.ASC : Order.DESC);
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
	
	static class DerivedQuery<T> extends AbstractDerivedQuery<T> {
		
		protected final EntityQueryCriteriaSupport<T, ?> executableEntityQuery;
		
		protected final EntityQueryPageSupport<T> dynamicSortSupport = new EntityQueryPageSupport<>();
		
		private DerivedQuery(AdvancedEntityPersister<T, ?> entityPersister, PartTree tree) {
			this.executableEntityQuery = entityPersister.newCriteriaSupport();
			tree.forEach(orPart -> orPart.forEach(this::append));
		}
		
		private DerivedQuery(EntityQueryCriteriaSupport<T, ?> executableEntityQuery) {
			this.executableEntityQuery = executableEntityQuery;
		}
		
		private void append(Part part) {
			Criterion criterion = convertToCriterion(part.getType());
			this.executableEntityQuery.getEntityCriteriaSupport().and(convertToAccessorChain(part.getProperty()), criterion.operator);
			super.criteriaChain.criteria.add(criterion);
		}
	}
}
