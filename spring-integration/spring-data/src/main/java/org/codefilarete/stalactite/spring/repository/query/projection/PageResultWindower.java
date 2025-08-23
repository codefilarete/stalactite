package org.codefilarete.stalactite.spring.repository.query.projection;

import java.util.List;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import org.codefilarete.stalactite.spring.repository.query.PartTreeStalactiteCountProjection;
import org.codefilarete.stalactite.spring.repository.query.StalactiteLimitRepositoryQuery;
import org.springframework.data.support.PageableExecutionUtils;

/**
 * {@link QueryResultWindower} dedicated to {@link org.springframework.data.domain.Page} result.
 * 
 * @param <C>
 * @param <R>
 * @param <P>
 * @author Guillaume Mary
 */
public class PageResultWindower<C, R, P> extends QueryResultWindower<C, R, P> {
	
	public PageResultWindower(StalactiteLimitRepositoryQuery<C, ?> delegate,
							  PartTreeStalactiteCountProjection<C> countQuery,
							  Supplier<List<P>> resultSupplier) {
		super(delegate,
				(accessor, result)
						-> (R) PageableExecutionUtils.getPage(result, accessor.getPageable(), () -> countQuery.execute(accessor.getValues())),
				resultSupplier);
	}
	
	public PageResultWindower(StalactiteLimitRepositoryQuery<C, ?> delegate,
							  LongSupplier countQuery,
							  Supplier<List<P>> resultSupplier) {
		super(delegate,
				(accessor, result)
						-> (R) PageableExecutionUtils.getPage(result, accessor.getPageable(), countQuery),
				resultSupplier);
	}
}
