package org.codefilarete.stalactite.spring.repository.query.nativ;

import java.util.Collection;
import java.util.function.Supplier;

public interface QueryExecutor<C extends Collection<R>, R> {

	Supplier<C> buildQueryExecutor(Object[] parameters);
}
