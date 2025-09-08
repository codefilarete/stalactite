package org.codefilarete.stalactite.spring.repository.query;

import java.util.Collection;

import org.codefilarete.stalactite.sql.Dialect;

public abstract class AbstractQueryExecutor<C extends Collection<R>, R> implements QueryExecutor<C, R> {

	protected final StalactiteQueryMethod method;
	protected final Dialect dialect;

	public AbstractQueryExecutor(StalactiteQueryMethod method, Dialect dialect) {
		this.method = method;
		this.dialect = dialect;
	}
}
