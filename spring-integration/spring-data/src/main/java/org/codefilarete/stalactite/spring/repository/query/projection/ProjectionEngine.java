package org.codefilarete.stalactite.spring.repository.query.projection;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;

public interface ProjectionEngine<R> {

	Function<Object[], R> adapt(Supplier<List<Map<String, Object>>> resultSupplier);
}
