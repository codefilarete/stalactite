package org.codefilarete.stalactite.spring.repository.query.execution.reduce;

import java.util.List;
import java.util.function.Supplier;

import org.codefilarete.stalactite.spring.repository.query.execution.StalactiteQueryMethodInvocationParameters;
import org.codefilarete.stalactite.spring.repository.query.StalactiteRepositoryQuery;
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

	public SliceResultWindower(StalactiteRepositoryQuery<C, ?> delegate,
							   LimitHandler limitHandler,
							   Supplier<List<P>> resultSupplier) {
		super(delegate,
				limitHandler,
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
	
	@Override
	protected void adaptLimit(StalactiteQueryMethodInvocationParameters invocationParameters) {
		Pageable pageable = invocationParameters.getPageable();
		if (pageable.getPageNumber() == 0) {
			// The + 1 is a look-ahead tip to make the returned Slice eventually return true on hasNext()
			limitHandler.limit(pageable.getPageSize() + 1);
		} else {
			// when the user asks for a page number (given Pageable is a Page instance or a Slice with page number) then we ask for the page number
			limitHandler.limit(pageable.getPageSize(), (int) pageable.getOffset());
		}
	}
}
