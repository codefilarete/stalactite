package org.codefilarete.stalactite.spring.repository.query.projection;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;

import org.codefilarete.stalactite.spring.repository.query.StalactiteLimitRepositoryQuery;
import org.springframework.data.domain.Slice;

public class SlicedProjectionEngine<C, R> implements ProjectionEngine<Slice<R>> {

	private final StalactiteLimitRepositoryQuery<C, ?> delegate;

	public SlicedProjectionEngine(StalactiteLimitRepositoryQuery<C, ?> delegate) {
		this.delegate = delegate;
	}

	@Override
	public Function<Object[], Slice<R>> adapt(Supplier<List<Map<String, Object>>> resultSupplier) {
		return new SliceResultWindower<C, Slice<R>, Map<String, Object>>(delegate, resultSupplier)::adaptExecution;
	}

}
