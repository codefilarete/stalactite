package org.codefilarete.stalactite.spring.repository.query.projection;

import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

public interface ProjectionEngine<R, I> {

	Function<Object[], R> adapt(Supplier<List<I>> resultSupplier);
}
