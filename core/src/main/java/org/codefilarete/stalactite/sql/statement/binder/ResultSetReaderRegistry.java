package org.codefilarete.stalactite.sql.statement.binder;

import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.statement.SQLStatement.BindingException;

/**
 * Contract for a registry of {@link ResultSetReader}s per {@link Column} and {@link Class}.
 * 
 * @author Guillaume Mary
 */
public interface ResultSetReaderRegistry extends ResultSetReaderProvider<Column> {
	
	default <T> ResultSetReader<T> getReader(Class<T> key) {
		ResultSetReader<T> writer = doGetReader(key);
		if (writer == null) {
			throw new BindingException("Reader for " + key + " is not registered");
		}
		return writer;
	}
	
	<T> ResultSetReader<T> doGetReader(Class<T> key);
}