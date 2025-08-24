package org.codefilarete.stalactite.spring.repository.query.reduce;

import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Supplier;

import org.codefilarete.stalactite.spring.repository.query.PartTreeStalactiteProjection;
import org.codefilarete.stalactite.spring.repository.query.PartTreeStalactiteQuery;
import org.codefilarete.stalactite.spring.repository.query.StalactiteQueryMethodInvocationParameters;
import org.codefilarete.stalactite.spring.repository.query.StalactiteRepositoryQuery;
import org.springframework.data.domain.Pageable;

/**
 * Technical class that applies windowing to a query result as Spring Data classes {@link org.springframework.data.domain.Slice}
 * and {@link org.springframework.data.domain.Page}.
 * The goal is to share the code that is necessary to {@link PartTreeStalactiteQuery} and {@link PartTreeStalactiteProjection}. However, its usage
 * is a bit complex (with callback, handler, etc.), thus it should be used with caution.
 * 
 * @param <C>
 * @param <R>
 * @param <P>
 * @author Guillaume Mary
 * @see PageResultWindower
 * @see SliceResultWindower
 */
public abstract class QueryResultWindower<C, R, P> {
	
	private final StalactiteRepositoryQuery<C, ?> delegate;
	protected final LimitHandler limitHandler;
	private final BiFunction<StalactiteQueryMethodInvocationParameters, List<P>, R> queryResultSlicer;
	private final Supplier<List<P>> resultSupplier;
	
	public QueryResultWindower(StalactiteRepositoryQuery<C, ?> delegate,
							   LimitHandler limitHandler,
							   BiFunction<StalactiteQueryMethodInvocationParameters, List<P>, R> queryResultSlicer,
							   Supplier<List<P>> resultSupplier) {
		this.delegate = delegate;
		this.limitHandler = limitHandler;
		this.queryResultSlicer = queryResultSlicer;
		this.resultSupplier = resultSupplier;
	}
	
	public R adaptExecution(Object[] parameters) {
		StalactiteQueryMethodInvocationParameters invocationParameters = new StalactiteQueryMethodInvocationParameters(delegate.getQueryMethod(), parameters);
		// windowing requires adapting the query to append a limit clause
		adaptLimit(invocationParameters);
		List<P> delegateResult = resultSupplier.get();
		return queryResultSlicer.apply(invocationParameters, delegateResult);
	}
	
	protected void adaptLimit(StalactiteQueryMethodInvocationParameters invocationParameters) {
		Pageable pageable = invocationParameters.getPageable();
		// when the user asks for a page number (given Pageable is a Page instance or a Slice with page number) then we ask for the page number
		limitHandler.limit(pageable.getPageSize(), (int) pageable.getOffset());
	}
}
