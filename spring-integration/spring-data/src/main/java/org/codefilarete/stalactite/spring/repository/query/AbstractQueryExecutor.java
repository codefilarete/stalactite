package org.codefilarete.stalactite.spring.repository.query;

import java.util.Collection;

public abstract class AbstractQueryExecutor<C extends Collection<R>, R> implements QueryExecutor<C, R> {

	protected final StalactiteQueryMethod method;

	public AbstractQueryExecutor(StalactiteQueryMethod method) {
		this.method = method;
	}
}
