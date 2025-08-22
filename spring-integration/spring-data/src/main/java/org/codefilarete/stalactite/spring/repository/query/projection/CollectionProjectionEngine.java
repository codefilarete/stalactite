package org.codefilarete.stalactite.spring.repository.query.projection;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;

public class CollectionProjectionEngine<R> implements ProjectionEngine<Collection<R>> {

	public CollectionProjectionEngine() {
	}

	@Override
	public Function<Object[], Collection<R>> adapt(Supplier<List<Map<String, Object>>> resultSupplier) {
		return objects -> (Collection<R>) resultSupplier.get();
	}

}
