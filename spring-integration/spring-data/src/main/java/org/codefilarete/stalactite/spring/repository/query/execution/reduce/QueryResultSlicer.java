package org.codefilarete.stalactite.spring.repository.query.execution.reduce;

import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

import org.codefilarete.stalactite.spring.repository.query.StalactiteRepositoryQuery;
import org.springframework.data.domain.Slice;

public class QueryResultSlicer<C, R, I> implements QueryResultReducer<Slice<R>, I> {
	
	private final StalactiteRepositoryQuery<C, ?> delegate;
	private final LimitHandler limitHandler;

	public QueryResultSlicer(StalactiteRepositoryQuery<C, ?> delegate, LimitHandler limitHandler) {
		this.delegate = delegate;
		this.limitHandler = limitHandler;
	}

	@Override
	public Function<Object[], Slice<R>> adapt(Supplier<List<I>> resultSupplier) {
		return new SliceResultWindower<C, Slice<R>, I>(delegate, limitHandler, resultSupplier)::adaptExecution;
	}

}
