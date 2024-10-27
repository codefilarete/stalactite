package org.codefilarete.stalactite.spring.repository.query;

import java.util.Set;

import org.codefilarete.stalactite.engine.EntityPersister;
import org.codefilarete.stalactite.engine.runtime.AdvancedEntityPersister;
import org.codefilarete.stalactite.sql.result.Accumulators;
import org.springframework.data.repository.query.QueryMethod;
import org.springframework.data.repository.query.RepositoryQuery;
import org.springframework.data.repository.query.parser.PartTree;

/**
 * {@link RepositoryQuery} for Stalactite delete order.
 * Deletion will follow mapping rules (no reason to differ from it), hence, this class wraps a
 * {@link PartTreeStalactiteQuery} that will collect the entities to be deleted, then the {@link EntityPersister#delete(Iterable)}
 * is used.
 *
 * @param <C> entity type
 * @author Guillaume Mary
 */
class PartTreeStalactiteDelete<C> implements RepositoryQuery {
	
	private final PartTreeStalactiteQuery<C, Set<C>> partTreeQuery;
	private final QueryMethod queryMethod;
	private final EntityPersister<C, ?> entityPersister;
	
	public PartTreeStalactiteDelete(QueryMethod queryMethod, AdvancedEntityPersister<C, ?> entityPersister, PartTree partTree) {
		this.partTreeQuery = new PartTreeStalactiteQuery<>(queryMethod, entityPersister, partTree, Accumulators.toSet());
		this.queryMethod = queryMethod;
		this.entityPersister = entityPersister;
	}
	
	@Override
	public Integer execute(Object[] parameters) {
		Set<C> execute = partTreeQuery.execute(parameters);
		entityPersister.delete(execute);
		return execute.size();
	}
	
	@Override
	public QueryMethod getQueryMethod() {
		return queryMethod;
	}
}
