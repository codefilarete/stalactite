package org.codefilarete.stalactite.engine.crud;

import org.codefilarete.stalactite.query.Operators;
import org.codefilarete.stalactite.sql.ddl.structure.Table;

/**
 * 
 * @param <T> table type
 * @author Guillaume Mary
 */
public interface ExecutableDelete<T extends Table<T>> {
	
	/**
	 * Sets a criteria value to this statement.
	 * Criteria are expected to be named and created by some {@link Operators} methods
	 * like {@link Operators#equalsArgNamed(String, Class)}.
	 *
	 * @param paramName name of the criteria
	 * @param value value for given parameter
	 * @param <O> value type
	 * @return this
	 */
	<O> ExecutableDelete<T> set(String paramName, O value);
	
	/**
	 * Executes this delete statement with given values.
	 */
	long execute();
}
