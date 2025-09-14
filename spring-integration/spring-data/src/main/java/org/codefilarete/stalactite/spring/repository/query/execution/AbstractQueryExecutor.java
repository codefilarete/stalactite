package org.codefilarete.stalactite.spring.repository.query.execution;

import java.util.Collection;

import org.codefilarete.stalactite.spring.repository.query.StalactiteQueryMethod;

public abstract class AbstractQueryExecutor<C extends Collection<R>, R> implements QueryExecutor<C, R> {

	protected final StalactiteQueryMethod method;

	public AbstractQueryExecutor(StalactiteQueryMethod method) {
		this.method = method;
	}
}
