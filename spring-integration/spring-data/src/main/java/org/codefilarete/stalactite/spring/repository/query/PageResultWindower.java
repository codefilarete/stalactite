package org.codefilarete.stalactite.spring.repository.query;

import java.util.List;
import java.util.function.Supplier;

import org.springframework.data.support.PageableExecutionUtils;

/**
 * {@link QueryResultWindower} dedicated to {@link org.springframework.data.domain.Page} result.
 * @param <C>
 * @param <I>
 * @param <R>
 * @param <P>
 * @author Guillaume Mary
 */
class PageResultWindower<C, I, R, P> extends QueryResultWindower<C, I, R, P> {
	
	PageResultWindower(StalactiteLimitRepositoryQuery<C, I> delegate, PartTreeStalactiteCountProjection<C> countQuery,
					   Supplier<List<P>> resultSupplier) {
		super(delegate,
				(accessor, result)
						-> (R) PageableExecutionUtils.getPage(result, accessor.getPageable(), () -> countQuery.execute(accessor.getValues())),
				resultSupplier);
	}
}
