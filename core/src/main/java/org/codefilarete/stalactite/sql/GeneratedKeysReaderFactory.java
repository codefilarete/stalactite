package org.codefilarete.stalactite.sql;

import org.codefilarete.stalactite.sql.statement.GeneratedKeysReader;

/**
 * @author Guillaume Mary
 */
public interface GeneratedKeysReaderFactory {
	
	<I> GeneratedKeysReader<I> build(String keyName, Class<I> columnType);
}
