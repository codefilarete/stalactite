package org.codefilarete.stalactite.spring.repository.query;

import java.util.List;
import java.util.function.Supplier;

import org.codefilarete.stalactite.engine.runtime.AdvancedEntityPersister;
import org.codefilarete.stalactite.spring.repository.query.projection.PageResultWindower;
import org.codefilarete.stalactite.spring.repository.query.projection.QueryResultWindower;
import org.codefilarete.stalactite.spring.repository.query.projection.SliceResultWindower;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Slice;
import org.springframework.data.repository.query.QueryMethod;
import org.springframework.data.repository.query.RepositoryQuery;
import org.springframework.data.repository.query.parser.PartTree;

/**
 * Kind of {@link PartTreeStalactiteQuery} made for partial result query like {@link Slice} and {@link Page} ones.
 * Designed as a wrapper of {@link StalactiteLimitRepositoryQuery} because its behavior is just a tweak of the real query.
 * 
 * @param <C>
 * @param <R>
 * @param <P>
 * @author Guillaume Mary
 */
public class PartTreeStalactitePagedQuery<C, R extends Slice<P>, P> implements RepositoryQuery {
	
	private final QueryMethod method;
	private final AdvancedEntityPersister<C, ?> entityPersister;
	private final PartTree tree;
	private final StalactiteLimitRepositoryQuery<C, List<P>> delegate;
	
	public PartTreeStalactitePagedQuery(QueryMethod method,
										AdvancedEntityPersister<C, ?> entityPersister,
										PartTree tree,
										StalactiteLimitRepositoryQuery<C, List<P>> delegate) {
		this.method = method;
		this.entityPersister = entityPersister;
		this.tree = tree;
		this.delegate = delegate;
	}
	
	@Override
	public R execute(Object[] parameters) {
		
		QueryResultWindower<C, R, P> queryResultWindower;
		Supplier<List<P>> resultSupplier = () -> delegate.execute(parameters);
		
		if (getQueryMethod().isSliceQuery()) {
			queryResultWindower = new SliceResultWindower<>(delegate, resultSupplier);
		} else if (getQueryMethod().isPageQuery()) {
			queryResultWindower = new PageResultWindower<>(delegate, new PartTreeStalactiteCountProjection<>(method, entityPersister, tree), resultSupplier);
		} else {
			// the result type might be a Collection or a single result
			queryResultWindower = new QueryResultWindower<C, R, P>(null, null, null) {
				@Override
				public R adaptExecution(Object[] parameters) {
					return (R) delegate.execute(parameters);
				}
			};
		}
		
		return queryResultWindower.adaptExecution(parameters);
	}
	
	@Override
	public QueryMethod getQueryMethod() {
		return delegate.getQueryMethod();
	}
}
