package org.codefilarete.stalactite.spring.repository.query;

import org.springframework.data.repository.query.RepositoryQuery;

public interface StalactiteLimitRepositoryQuery<C, R> extends StalactiteRepositoryQuery<C, R> {
	
	void limit(int count);
	
	void limit(int count, Integer offset);
}
