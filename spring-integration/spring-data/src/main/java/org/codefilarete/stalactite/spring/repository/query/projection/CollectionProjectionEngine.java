package org.codefilarete.stalactite.spring.repository.query.projection;

import java.util.Collection;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

public class CollectionProjectionEngine<R, I> implements ProjectionEngine<Collection<R>, I> {

	public CollectionProjectionEngine() {
	}

	@Override
	public Function<Object[], Collection<R>> adapt(Supplier<List<I>> resultSupplier) {
		return objects -> (Collection<R>) resultSupplier.get();
	}

}
