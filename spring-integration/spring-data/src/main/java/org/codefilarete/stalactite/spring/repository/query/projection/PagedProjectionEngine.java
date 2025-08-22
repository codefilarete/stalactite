package org.codefilarete.stalactite.spring.repository.query.projection;

import java.util.List;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import org.codefilarete.stalactite.spring.repository.query.PartTreeStalactiteCountProjection;
import org.codefilarete.stalactite.spring.repository.query.StalactiteLimitRepositoryQuery;
import org.springframework.data.domain.Page;

import static org.codefilarete.stalactite.spring.repository.query.projection.PartTreeStalactiteLimitingQuery.StalactiteParametersParameterAccessor;

public class PagedProjectionEngine<C, R, I> implements ProjectionEngine<Page<R>, I> {

	private final StalactiteLimitRepositoryQuery<C, ?> delegate;
	private final Function<Object[], LongSupplier> countSupplier;

	public PagedProjectionEngine(StalactiteLimitRepositoryQuery<C, ?> delegate,
								 LongSupplier countSupplier) {
		this.delegate = delegate;
		this.countSupplier = (parameters) -> countSupplier;
	}

	public PagedProjectionEngine(StalactiteLimitRepositoryQuery<C, ?> delegate,
								 PartTreeStalactiteCountProjection<C> countQuery) {
		this.delegate = delegate;
		this.countSupplier = (parameters) -> {
					StalactiteParametersParameterAccessor smartParameters = new StalactiteParametersParameterAccessor(delegate.getQueryMethod().getParameters(), parameters);
					return () -> countQuery.execute(smartParameters.getValues());
				};
	}

	@Override
	public Function<Object[], Page<R>> adapt(Supplier<List<I>> resultSupplier) {
		return parameters -> new PageResultWindower<C, Page<R>, I>(delegate, countSupplier.apply(parameters), resultSupplier).adaptExecution(parameters);
	}

}
