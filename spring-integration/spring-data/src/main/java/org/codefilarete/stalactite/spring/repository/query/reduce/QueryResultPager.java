package org.codefilarete.stalactite.spring.repository.query.reduce;

import java.util.List;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import org.codefilarete.stalactite.spring.repository.query.PartTreeStalactiteCountProjection;
import org.codefilarete.stalactite.spring.repository.query.StalactiteQueryMethodInvocationParameters;
import org.codefilarete.stalactite.spring.repository.query.StalactiteRepositoryQuery;
import org.springframework.data.domain.Page;

public class QueryResultPager<C, R, I> implements QueryResultReducer<Page<R>, I> {

	private final StalactiteRepositoryQuery<C, ?> delegate;
	private final LimitHandler limitHandler;
	private final Function<Object[], LongSupplier> countSupplier;

	public QueryResultPager(StalactiteRepositoryQuery<C, ?> delegate,
							LimitHandler limitHandler,
							LongSupplier countSupplier) {
		this.delegate = delegate;
		this.limitHandler = limitHandler;
		this.countSupplier = (parameters) -> countSupplier;
	}

	public QueryResultPager(StalactiteRepositoryQuery<C, ?> delegate,
							LimitHandler limitHandler,
							PartTreeStalactiteCountProjection<C> countQuery) {
		this.delegate = delegate;
		this.limitHandler = limitHandler;
		this.countSupplier = (parameters) -> {
					StalactiteQueryMethodInvocationParameters smartParameters = new StalactiteQueryMethodInvocationParameters(delegate.getQueryMethod(), parameters);
					return () -> countQuery.execute(smartParameters.getValues());
				};
	}

	@Override
	public Function<Object[], Page<R>> adapt(Supplier<List<I>> resultSupplier) {
		return parameters -> new PageResultWindower<C, Page<R>, I>(delegate, limitHandler, countSupplier.apply(parameters), resultSupplier).adaptExecution(parameters);
	}

}
