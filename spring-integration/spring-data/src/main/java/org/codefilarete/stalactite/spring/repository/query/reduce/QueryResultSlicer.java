package org.codefilarete.stalactite.spring.repository.query.reduce;

import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

import org.codefilarete.stalactite.spring.repository.query.StalactiteLimitRepositoryQuery;
import org.springframework.data.domain.Slice;

public class QueryResultSlicer<C, R, I> implements QueryResultReducer<Slice<R>, I> {

	private final StalactiteLimitRepositoryQuery<C, ?> delegate;

	public QueryResultSlicer(StalactiteLimitRepositoryQuery<C, ?> delegate) {
		this.delegate = delegate;
	}

	@Override
	public Function<Object[], Slice<R>> adapt(Supplier<List<I>> resultSupplier) {
		return new SliceResultWindower<C, Slice<R>, I>(delegate, resultSupplier)::adaptExecution;
	}

}
