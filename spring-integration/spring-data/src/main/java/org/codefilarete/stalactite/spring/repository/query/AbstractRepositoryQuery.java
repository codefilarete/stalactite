package org.codefilarete.stalactite.spring.repository.query;

import org.springframework.data.repository.query.QueryMethod;
import org.springframework.data.repository.query.RepositoryQuery;

/**
 * Parent class of some {@link RepositoryQuery} to share some code
 * @author Guillaume Mary
 */
public abstract class AbstractRepositoryQuery implements RepositoryQuery {

	protected final QueryMethod method;

	public AbstractRepositoryQuery(QueryMethod method) {
		this.method = method;
	}

	@Override
	public QueryMethod getQueryMethod() {
		return method;
	}

}
