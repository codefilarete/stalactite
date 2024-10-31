package org.codefilarete.stalactite.spring.autoconfigure;

import org.codefilarete.stalactite.sql.Dialect;

/**
 * 
 * @author Guillaume Mary
 */
public interface DialectCustomizer {
	
	void customize(Dialect dialect);
}
