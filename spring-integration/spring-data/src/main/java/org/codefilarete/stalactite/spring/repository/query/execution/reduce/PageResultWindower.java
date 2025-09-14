package org.codefilarete.stalactite.spring.repository.query.execution.reduce;

import java.util.List;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import org.codefilarete.stalactite.spring.repository.query.projection.PartTreeStalactiteCountProjection;
import org.codefilarete.stalactite.spring.repository.query.StalactiteRepositoryQuery;
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
	
	public PageResultWindower(StalactiteRepositoryQuery<C, ?> delegate,
							  LimitHandler limitHandler,
							  PartTreeStalactiteCountProjection<C> countQuery,
							  Supplier<List<P>> resultSupplier) {
		super(delegate,
				limitHandler,
				(accessor, result)
						-> (R) PageableExecutionUtils.getPage(result, accessor.getPageable(), () -> countQuery.execute(accessor.getValues())),
				resultSupplier);
	}
	
	public PageResultWindower(StalactiteRepositoryQuery<C, ?> delegate,
							  LimitHandler limitHandler,
							  LongSupplier countQuery,
							  Supplier<List<P>> resultSupplier) {
		super(delegate,
				limitHandler,
				(accessor, result)
						-> (R) PageableExecutionUtils.getPage(result, accessor.getPageable(), countQuery),
				resultSupplier);
	}
}
