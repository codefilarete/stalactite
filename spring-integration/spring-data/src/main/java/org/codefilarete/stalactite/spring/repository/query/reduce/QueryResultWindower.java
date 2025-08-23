package org.codefilarete.stalactite.spring.repository.query.reduce;

import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Supplier;

import org.codefilarete.stalactite.spring.repository.query.PartTreeStalactiteProjection;
import org.codefilarete.stalactite.spring.repository.query.PartTreeStalactiteQuery;
import org.codefilarete.stalactite.spring.repository.query.StalactiteLimitRepositoryQuery;
import org.codefilarete.stalactite.spring.repository.query.StalactiteParametersParameterAccessor;
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
	
	private final StalactiteLimitRepositoryQuery<C, ?> delegate;
	protected final BiFunction<StalactiteParametersParameterAccessor, List<P>, R> queryResultSlicer;
	private final Supplier<List<P>> resultSupplier;
	
	public QueryResultWindower(StalactiteLimitRepositoryQuery<C, ?> delegate,
							   BiFunction<StalactiteParametersParameterAccessor, List<P>, R> queryResultSlicer,
							   Supplier<List<P>> resultSupplier) {
		this.delegate = delegate;
		this.queryResultSlicer = queryResultSlicer;
		this.resultSupplier = resultSupplier;
	}
	
	public R adaptExecution(Object[] parameters) {
		StalactiteParametersParameterAccessor smartParameters = new StalactiteParametersParameterAccessor(delegate.getQueryMethod().getParameters(), parameters);
		// windowing requires adapting the query to append a limit clause
		adaptLimit(smartParameters);
		List<P> delegateResult = resultSupplier.get();
		return queryResultSlicer.apply(smartParameters, delegateResult);
	}
	
	private void adaptLimit(StalactiteParametersParameterAccessor smartParameters) {
		Pageable pageable = smartParameters.getPageable();
		if (delegate.getQueryMethod().isSliceQuery() && pageable.getPageNumber() == 0) {
			// The + 1 is a look-ahead tip to make the returned Slice eventually return true on hasNext()
			delegate.limit(pageable.getPageSize() + 1);
		} else {
			// when the user asks for a page number (given Pageable is a Page instance or a Slice with page number) then we ask for the page number
			delegate.limit(pageable.getPageSize(), (int) pageable.getOffset());
		}
	}
}
