package org.codefilarete.stalactite.spring.repository.query.projection;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;

public class SingleProjectionEngine<R> implements ProjectionEngine<R> {

	public SingleProjectionEngine() {
	}

	@Override
	public Function<Object[], R> adapt(Supplier<List<Map<String, Object>>> resultSupplier) {
		return objects -> (R) resultSupplier.get().get(0);
	}

}
