package org.codefilarete.stalactite.spring.repository.query.projection;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import org.codefilarete.stalactite.spring.repository.query.PartTreeStalactiteCountProjection;
import org.codefilarete.stalactite.spring.repository.query.StalactiteLimitRepositoryQuery;
import org.springframework.data.domain.Slice;

import static org.codefilarete.stalactite.spring.repository.query.PartTreeStalactiteLimitingQuery.*;

public class PagedProjectionEngine<C, R> implements ProjectionEngine<Slice<R>> {

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
	public Function<Object[], Slice<R>> adapt(Supplier<List<Map<String, Object>>> resultSupplier) {
		return objects -> new PageResultWindower<C, Slice<R>, Map<String, Object>>(delegate, countSupplier.apply(objects), resultSupplier).adaptExecution(objects);
	}

}
