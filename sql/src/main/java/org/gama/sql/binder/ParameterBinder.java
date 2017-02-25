package org.gama.sql.binder;

/**
 * Merge of {@link ResultSetReader} et {@link PreparedStatementWriter}
 * 
 * @author Guillaume Mary
 */
public interface ParameterBinder<T> extends ResultSetReader<T>, PreparedStatementWriter<T> {
	
}
