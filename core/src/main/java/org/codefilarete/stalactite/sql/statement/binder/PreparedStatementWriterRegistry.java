package org.codefilarete.stalactite.sql.statement.binder;

import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.statement.SQLStatement.BindingException;

/**
 * Contract for a registry of {@link PreparedStatementWriter}s per {@link Column} and {@link Class}.
 * 
 * @author Guillaume Mary
 */
public interface PreparedStatementWriterRegistry extends PreparedStatementWriterProvider<Column> {
	
	default <T> PreparedStatementWriter<T> getWriter(Class<T> key) {
		PreparedStatementWriter<T> writer = doGetWriter(key);
		if (writer == null) {
			throw new BindingException("Writer for " + key + " is not registered");
		}
		return writer;
	}
	
	<T> PreparedStatementWriter<T> doGetWriter(Class<T> key);
}
