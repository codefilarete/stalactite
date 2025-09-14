package org.codefilarete.stalactite.spring.repository.query.execution.reduce;

public interface LimitHandler {
	
	void limit(int count);
	
	void limit(int count, Integer offset);
}
