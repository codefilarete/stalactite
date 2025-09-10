package org.codefilarete.stalactite.spring.repository.query;

import java.util.Collection;

import org.codefilarete.stalactite.engine.EntityPersister;
import org.codefilarete.stalactite.engine.runtime.AdvancedEntityPersister;
import org.codefilarete.stalactite.spring.repository.query.reduce.QueryResultCollectioner;
import org.codefilarete.stalactite.spring.repository.query.reduce.QueryResultReducer;
import org.codefilarete.stalactite.sql.Dialect;
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
	
	private final PartTreeStalactiteQuery<C, Collection<C>> partTreeQuery;
	private final QueryMethod queryMethod;
	private final AdvancedEntityPersister<C, ?> entityPersister;
	
	public PartTreeStalactiteDelete(StalactiteQueryMethod queryMethod, AdvancedEntityPersister<C, ?> entityPersister, PartTree partTree, Dialect dialect) {
		this.partTreeQuery = new PartTreeStalactiteQuery<C, Collection<C>>(queryMethod, entityPersister, partTree, dialect) {

			@Override
			protected <ROW> QueryResultReducer<Collection<C>, ROW> buildResultReducer(StalactiteQueryMethodInvocationParameters invocationParameters) {
				return new QueryResultCollectioner<>();
			}
		};
		this.queryMethod = queryMethod;
		this.entityPersister = entityPersister;
	}
	
	@Override
	public Integer execute(Object[] parameters) {
		Collection<C> execute = partTreeQuery.execute(parameters);
		if (execute == null || execute.isEmpty()) {
			return 0;
		} else {
			entityPersister.delete(execute);
			return execute.size();
		}
	}
	
	@Override
	public QueryMethod getQueryMethod() {
		return queryMethod;
	}
}
