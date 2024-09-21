package org.codefilarete.stalactite.sql.spring.repository.query;

import org.codefilarete.reflection.AccessorChain;
import org.codefilarete.stalactite.engine.EntityPersister;
import org.codefilarete.stalactite.engine.EntityPersister.ExecutableEntityQuery;
import org.codefilarete.stalactite.engine.EntityPersister.OrderByChain.Order;
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
	protected final Query<C> query;
	protected final Accumulator<C, ?, R> accumulator;
	
	public PartTreeStalactiteQuery(QueryMethod method, EntityPersister<C, ?> entityPersister, PartTree tree, Accumulator<C, ?, R> accumulator) {
		this.method = method;
		this.accumulator = accumulator;
		Parameters<?, ?> parameters = method.getParameters();
		
		boolean recreationRequired = parameters.potentiallySortsDynamically();
		
		try {
			this.query = new Query<>(entityPersister, tree);
			// Applying sort if necessary
			if (tree.getSort().isSorted()) {
				tree.getSort().iterator().forEachRemaining(order -> {
					PropertyPath propertyPath = PropertyPath.from(order.getProperty(), entityPersister.getClassToPersist());
					AccessorChain<C, Object> orderProperty = query.convertToAccessorChain(propertyPath);
					query.executableEntityQuery
							.orderBy(orderProperty, order.getDirection() == Direction.ASC ? Order.ASC : Order.DESC);
				});
			}
			// Applying limit if necessary
			nullable(tree.getMaxResults()).invoke(query.executableEntityQuery::limit);
		} catch (RuntimeException o_O) {
			throw new IllegalArgumentException(
					String.format("Failed to create query for method %s! %s", method, o_O.getMessage()), o_O);
		}
	}
	
	@Override
	@Nullable
	public R execute(Object[] parameters) {
		query.criteriaChain.consume(parameters);
		R result = query.executableEntityQuery.execute(accumulator);
		
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
	
	@Override
	public QueryMethod getQueryMethod() {
		return method;
	}
	
	class Query<T> extends AbstractQuery<T> {
		
		protected final ExecutableEntityQuery<T, ?> executableEntityQuery;
		
		Query(EntityPersister<T, ?> entityPersister, PartTree tree) {
			super(entityPersister);
			executableEntityQuery = entityPersister.selectWhere();
			tree.forEach(orPart -> orPart.forEach(this::append));
		}
		
		private void append(Part part) {
			Criterion criterion = convertToOperator(part.getType());
			executableEntityQuery.and(convertToAccessorChain(part.getProperty()), criterion.operator);
			super.criteriaChain.criteria.add(criterion);
		}
	}
	
}
