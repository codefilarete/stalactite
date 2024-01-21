package org.codefilarete.stalactite.sql.statement.binder;

import org.codefilarete.stalactite.sql.statement.SQLStatement.BindingException;

@FunctionalInterface
public interface ResultSetReaderProvider<K> {
	
	/**
	 * Gives a {@link ParameterBinder} from a key.
	 * Will throw an exception in case of missing {@link ParameterBinder}
	 *
	 * @param key an object for which a {@link ParameterBinder} is expected
	 * @return the {@link ParameterBinder} associated with the key 
	 */
	default ResultSetReader getReader(K key) {
		ResultSetReader writer = doGetReader(key);
		if (writer == null) {
			throw new BindingException("Reader for " + key + " is not registered");
		}
		return writer;
	}
	
	ResultSetReader doGetReader(K key);
}
