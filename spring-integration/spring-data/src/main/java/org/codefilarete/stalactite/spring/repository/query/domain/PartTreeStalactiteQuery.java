package org.codefilarete.stalactite.spring.repository.query.domain;

import java.util.List;
import java.util.function.LongSupplier;

import org.codefilarete.stalactite.engine.runtime.AdvancedEntityPersister;
import org.codefilarete.stalactite.spring.repository.query.AbstractQueryExecutor;
import org.codefilarete.stalactite.spring.repository.query.AbstractRepositoryQuery;
import org.codefilarete.stalactite.spring.repository.query.StalactiteQueryMethod;
import org.codefilarete.stalactite.spring.repository.query.StalactiteQueryMethodInvocationParameters;
import org.codefilarete.stalactite.spring.repository.query.projection.PartTreeStalactiteCountProjection;
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
	private final PartTreeStalactiteCountProjection<C> countQuery;
	
	public PartTreeStalactiteQuery(StalactiteQueryMethod method,
								   AdvancedEntityPersister<C, ?> entityPersister,
								   PartTree partTree) {
		super(method);
		this.entityPersister = entityPersister;
		this.partTree = partTree;
		this.countQuery = new PartTreeStalactiteCountProjection<>(method, entityPersister, partTree);
	}
	
	@Override
	protected AbstractQueryExecutor<List<Object>, Object> buildQueryExecutor(StalactiteQueryMethodInvocationParameters invocationParameters) {
		return (AbstractQueryExecutor) new DomainEntityQueryExecutor<>(method, entityPersister, partTree);
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
