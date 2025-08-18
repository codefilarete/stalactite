package org.codefilarete.stalactite.spring.repository.query;

import org.springframework.data.repository.query.RepositoryQuery;

public interface StalactiteRepositoryQuery<C, R> extends RepositoryQuery {
	
	@Override
	R execute(Object[] parameters);
}
