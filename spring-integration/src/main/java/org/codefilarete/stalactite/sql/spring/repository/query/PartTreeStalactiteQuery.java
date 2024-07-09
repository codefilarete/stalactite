package org.codefilarete.stalactite.sql.spring.repository.query;

import org.codefilarete.stalactite.engine.EntityPersister;
import org.codefilarete.stalactite.engine.EntityPersister.ExecutableEntityQuery;
import org.codefilarete.stalactite.sql.result.Accumulator;
import org.codefilarete.tool.function.Hanger.Holder;
import org.codefilarete.tool.trace.ModifiableInt;
import org.springframework.data.jpa.repository.query.PartTreeJpaQuery;
import org.springframework.data.repository.query.Parameters;
import org.springframework.data.repository.query.QueryMethod;
import org.springframework.data.repository.query.RepositoryQuery;
import org.springframework.data.repository.query.parser.Part;
import org.springframework.data.repository.query.parser.PartTree;
import org.springframework.lang.Nullable;

/**
 * {@link RepositoryQuery} for Stalactite. Inspired by {@link PartTreeJpaQuery}.
 * The parsing of the {@link QueryMethod} is made by Spring {@link PartTree}, hence this class only iterates over parts
 * to create a Stalactite query.
 * 
 * @param <C> entity type
 * @author Guillaume Mary
 */
public class PartTreeStalactiteQuery<C, R> implements RepositoryQuery {
	
	private final QueryMethod method;
	private final Query<C> query;
	private final Accumulator<C, ?, R> accumulator;
	
	public PartTreeStalactiteQuery(QueryMethod method, EntityPersister<C, ?> entityPersister, PartTree tree, Accumulator<C, ?, R> accumulator) {
		this.method = method;
		this.accumulator = accumulator;
		Parameters<?, ?> parameters = method.getParameters();
		
		boolean recreationRequired = parameters.potentiallySortsDynamically();
		
		try {
			this.query = new Query<>(entityPersister, tree);
			
		} catch (RuntimeException o_O) {
			throw new IllegalArgumentException(
					String.format("Failed to create query for method %s! %s", method, o_O.getMessage()), o_O);
		}
	}
	
	@Override
	@Nullable
	public R execute(Object[] parameters) {
		query.criteriaChain.consume(parameters);
		return query.executableEntityQuery.execute(accumulator);
	}
	
	@Override
	public QueryMethod getQueryMethod() {
		return method;
	}
	
	static class Query<T> extends AbstractQuery<T> {
		
		private final ExecutableEntityQuery<T> executableEntityQuery;
		
		Query(EntityPersister<T, ?> entityPersister, PartTree tree) {
			super(entityPersister);
			Holder<ExecutableEntityQuery<T>> resultHolder = new Holder<>();
			ModifiableInt partIndex = new ModifiableInt();
			tree.forEach(orPart -> {
				orPart.forEach(part -> {
							append(part, resultHolder, partIndex.getValue());
							partIndex.increment();
						}
				);
			});
			executableEntityQuery = resultHolder.get();
		}
		
		private void append(Part part, Holder<ExecutableEntityQuery<T>> resultHolder, int partIndex) {
			Criterion criterion = convertToOperator(part.getType());
			// entityPersister doesn't support the creation of a ExecutableEntityQuery from scratch and requires to
			// create it from entityPersister.selectWhere : we call it for first part
			if (partIndex == 0) {
				ExecutableEntityQuery<T> criteriaHook = entityPersister.selectWhere(convertToAccessorChain(part.getProperty()), criterion.operator);
				resultHolder.set(criteriaHook);
			} else {
				resultHolder.get().and(convertToAccessorChain(part.getProperty()), criterion.operator);
			}
			this.criteriaChain.criteria.add(criterion);
		}
		
	}
}
