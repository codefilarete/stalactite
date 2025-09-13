package org.codefilarete.stalactite.spring.repository.query;

import java.util.Collection;
import java.util.function.Supplier;

public interface QueryExecutor<C extends Collection<R>, R> {

	Supplier<C> buildQueryExecutor(StalactiteQueryMethodInvocationParameters invocationParameters);
}
