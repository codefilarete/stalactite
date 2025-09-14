package org.codefilarete.stalactite.spring.repository.query.domain;

import java.util.List;
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
import org.codefilarete.stalactite.spring.repository.query.execution.AbstractQueryExecutor;
import org.codefilarete.stalactite.spring.repository.query.StalactiteQueryMethod;
import org.codefilarete.stalactite.spring.repository.query.execution.StalactiteQueryMethodInvocationParameters;
import org.codefilarete.stalactite.spring.repository.query.derivation.ToCriteriaPartTreeTransformer;
import org.codefilarete.stalactite.spring.repository.query.derivation.ToCriteriaPartTreeTransformer.Condition;
import org.codefilarete.stalactite.sql.result.Accumulators;
import org.springframework.data.domain.Sort.Direction;
import org.springframework.data.repository.query.parser.PartTree;

/**
 * Implementation of {@link AbstractQueryExecutor} dedicated to domain entities.
 * Implementation is based on {@link EntityQueryCriteriaSupport} provided by the {@link AdvancedEntityPersister}.
 *
 * @param <C> domain entity type
 * @author Guillaume Mary
 */
public class DomainEntityQueryExecutor<C> extends AbstractQueryExecutor<List<C>, C> {
	
	private final AdvancedEntityPersister<C, ?> entityPersister;
	private final ToCriteriaPartTreeTransformer<C> criteriaAppender;
	
	public DomainEntityQueryExecutor(StalactiteQueryMethod method,
									 AdvancedEntityPersister<C, ?> entityPersister,
									 PartTree partTree) {
		super(method);
		this.entityPersister = entityPersister;
		this.criteriaAppender = new ToCriteriaPartTreeTransformer<>(
				partTree,
				entityPersister.getClassToPersist());
	}
	
	@Override
	public Supplier<List<C>> buildQueryExecutor(StalactiteQueryMethodInvocationParameters invocationParameters) {
		return () -> {
			EntityQueryCriteriaSupport<C, ?> executableEntityQuery = entityPersister.newCriteriaSupport();
			Condition condition = criteriaAppender.applyTo(
					executableEntityQuery.getEntityCriteriaSupport(),
					executableEntityQuery.getQueryPageSupport(),
					executableEntityQuery.getQueryPageSupport());
			condition.consume(invocationParameters.getValues());
			
			ExecutableEntityQueryCriteria<C, ?> executableEntityQueryCriteria = handleDynamicParameters(invocationParameters, executableEntityQuery);
			
			List<C> adaptation = executableEntityQueryCriteria.execute(Accumulators.toList());
			return method.getResultProcessor().processResult(adaptation);
		};
	}
	
	private ExecutableEntityQueryCriteria<C, ?> handleDynamicParameters(StalactiteQueryMethodInvocationParameters invocationParameters,
																		EntityQueryCriteriaSupport<C, ?> defaultExecutableEntityQuery) {
		EntityQueryCriteriaSupport<C, ?> derivedQueryToUse;
		// following code will manage both Sort as an argument, and Sort in a Pageable because getSort() handle both
		if (invocationParameters.getSort().isSorted()) {
			Class<?> declaringClass = method.getEntityInformation().getJavaType();
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
}
