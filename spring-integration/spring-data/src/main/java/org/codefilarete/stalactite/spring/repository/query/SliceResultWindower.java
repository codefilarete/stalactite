package org.codefilarete.stalactite.spring.repository.query;

import java.util.List;
import java.util.function.Supplier;

import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.SliceImpl;

/**
 * {@link QueryResultWindower} dedicated to {@link org.springframework.data.domain.Slice} result.
 * @param <C>
 * @param <I>
 * @param <R>
 * @param <P>
 * @author Guillaume Mary
 */
class SliceResultWindower<C, I, R, P> extends QueryResultWindower<C, I, R, P> {
	
	SliceResultWindower(StalactiteLimitRepositoryQuery<C, I> delegate,
						Supplier<List<P>> resultSupplier) {
		super(delegate,
				(accessor, result) -> {
					int pageSize = 0;
					Pageable pageable = accessor.getPageable();
					if (pageable.isPaged()) {
						pageSize = pageable.getPageSize();
					}
					boolean hasNext = pageable.isPaged() && result.size() > pageSize;
					return (R) new SliceImpl<>(hasNext ? result.subList(0, pageSize) : result, pageable, hasNext);
				},
				resultSupplier);
	}
}
