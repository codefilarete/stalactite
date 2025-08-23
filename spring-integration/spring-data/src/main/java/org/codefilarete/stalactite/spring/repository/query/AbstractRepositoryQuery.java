package org.codefilarete.stalactite.spring.repository.query;

import org.springframework.data.repository.query.RepositoryQuery;

/**
 * Parent class of some {@link RepositoryQuery} to share some code
 * @author Guillaume Mary
 */
public abstract class AbstractRepositoryQuery implements RepositoryQuery {

	protected final StalactiteQueryMethod method;

	public AbstractRepositoryQuery(StalactiteQueryMethod method) {
		this.method = method;
	}

	@Override
	public StalactiteQueryMethod getQueryMethod() {
		return method;
	}

}