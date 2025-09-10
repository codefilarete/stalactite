package org.codefilarete.stalactite.spring.repository.query;

import java.util.List;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import org.codefilarete.reflection.AccessorByMember;
import org.codefilarete.reflection.AccessorChain;
import org.codefilarete.reflection.Accessors;
import org.codefilarete.stalactite.engine.EntityPersister.OrderByChain.Order;
import org.codefilarete.stalactite.engine.runtime.AdvancedEntityPersister;
import org.codefilarete.stalactite.engine.runtime.RelationalEntityPersister.ExecutableEntityQueryCriteria;
import org.codefilarete.stalactite.engine.runtime.query.EntityQueryCriteriaSupport;
import org.codefilarete.stalactite.engine.runtime.query.EntityQueryCriteriaSupport.EntityQueryPageSupport;
import org.codefilarete.stalactite.query.model.Limit;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.result.Accumulators;
import org.springframework.data.domain.Sort.Direction;
import org.springframework.data.repository.query.QueryMethod;
import org.springframework.data.repository.query.RepositoryQuery;
import org.springframework.data.repository.query.parser.PartTree;

/**
 * {@link RepositoryQuery} for Stalactite. Inspired by {@link org.springframework.data.jpa.repository.query.PartTreeJpaQuery}.
 * The parsing of the {@link QueryMethod} is made by Spring {@link PartTree}, hence this class only iterates over parts
 * to create a Stalactite query.
 *
 * @param <C> entity type
 * @author Guillaume Mary
 */
public class PartTreeStalactiteQuery<C, R> extends AbstractRepositoryQuery<C, R> {
	
	private final AdvancedEntityPersister<C, ?> entityPersister;
	private final PartTree partTree;
	private final Dialect dialect;
	private final PartTreeStalactiteCountProjection<C> countQuery;
	
	public PartTreeStalactiteQuery(StalactiteQueryMethod method,
								   AdvancedEntityPersister<C, ?> entityPersister,
								   PartTree partTree,
								   Dialect dialect) {
		super(method);
		this.entityPersister = entityPersister;
		this.partTree = partTree;
		this.dialect = dialect;
		this.countQuery = new PartTreeStalactiteCountProjection<>(method, entityPersister, partTree);
	}
	
	@Override
	protected AbstractQueryExecutor<List<Object>, Object> buildQueryExecutor(StalactiteQueryMethodInvocationParameters invocationParameters) {
		return new AbstractQueryExecutor<List<Object>, Object>(method, dialect) {
			
			@Override
			public Supplier<List<Object>> buildQueryExecutor(Object[] parameters) {
				return () -> {
					EntityQueryCriteriaSupport<C, ?> executableEntityQuery = entityPersister.newCriteriaSupport();
					ToCriteriaPartTreeTransformer<C> criteriaAppender = new ToCriteriaPartTreeTransformer<>(
							partTree,
							entityPersister.getClassToPersist(),
							executableEntityQuery.getEntityCriteriaSupport(),
							executableEntityQuery.getQueryPageSupport(),
							executableEntityQuery.getQueryPageSupport());
					criteriaAppender.condition.consume(parameters);
					
					ExecutableEntityQueryCriteria<C, ?> executableEntityQueryCriteria = handleDynamicParameters(invocationParameters, executableEntityQuery);
					
					List<C> adaptation = executableEntityQueryCriteria.execute(Accumulators.toList());
					return method.getResultProcessor().processResult(adaptation);
				};
			}
		};
	}
	
	private ExecutableEntityQueryCriteria<C, ?> handleDynamicParameters(StalactiteQueryMethodInvocationParameters invocationParameters,
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
		ExecutableEntityQueryCriteria<C, ?> result = derivedQueryToUse.wrapIntoExecutable();
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
	
	@Override
	public StalactiteQueryMethod getQueryMethod() {
		return method;
	}
}
