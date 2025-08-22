package org.codefilarete.stalactite.spring.repository.query.projection;

import java.util.List;
import java.util.function.Supplier;

import org.codefilarete.stalactite.spring.repository.query.StalactiteLimitRepositoryQuery;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.SliceImpl;

/**
 * {@link QueryResultWindower} dedicated to {@link org.springframework.data.domain.Slice} result.
 * @param <C>
 * @param <R>
 * @param <P>
 * @author Guillaume Mary
 */
public class SliceResultWindower<C, R, P> extends QueryResultWindower<C, R, P> {

	public SliceResultWindower(StalactiteLimitRepositoryQuery<C, ?> delegate,
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
