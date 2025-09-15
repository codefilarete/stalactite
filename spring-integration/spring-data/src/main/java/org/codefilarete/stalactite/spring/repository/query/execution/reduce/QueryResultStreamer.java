package org.codefilarete.stalactite.spring.repository.query.execution.reduce;

import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.codefilarete.stalactite.spring.repository.query.StalactiteRepositoryQuery;
import org.codefilarete.stalactite.spring.repository.query.execution.StalactiteQueryMethodInvocationParameters;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Slice;
import org.springframework.data.domain.SliceImpl;

public class QueryResultStreamer<C, R, I> implements QueryResultReducer<Stream<R>, I> {
	
	private final StalactiteRepositoryQuery<C, ?> delegate;
	private final LimitHandler limitHandler;

	public QueryResultStreamer(StalactiteRepositoryQuery<C, ?> delegate, LimitHandler limitHandler) {
		this.delegate = delegate;
		this.limitHandler = limitHandler;
	}

	@Override
	public Function<Object[], Stream<R>> adapt(Supplier<List<I>> resultSupplier) {
		return new StreamResultWindower<C, Stream<R>, I>(delegate, limitHandler, resultSupplier)::adaptExecution;
	}
	
	static private class StreamResultWindower<C, R, P> extends QueryResultWindower<C, R, P> {
		
		public StreamResultWindower(StalactiteRepositoryQuery<C, ?> delegate,
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
						result = hasNext ? result.subList(0, pageSize) : result;
						return (R) result.stream();
					},
					resultSupplier);
		}
		
		@Override
		protected void adaptLimit(StalactiteQueryMethodInvocationParameters invocationParameters) {
			Pageable pageable = invocationParameters.getPageable();
			if (!pageable.isUnpaged()) {
				if (pageable.getPageNumber() == 0) {
					// The + 1 is a look-ahead tip to make the returned Slice eventually return true on hasNext()
					limitHandler.limit(pageable.getPageSize() + 1);
				} else {
					// when the user asks for a page number (given Pageable is a Page instance or a Slice with page number) then we ask for the page number
					limitHandler.limit(pageable.getPageSize(), (int) pageable.getOffset());
				}
			}
			// else: not paging is expected, thus no limit is required
		}
	}
}
