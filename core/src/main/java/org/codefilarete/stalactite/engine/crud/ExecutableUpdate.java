package org.codefilarete.stalactite.engine.crud;

import org.codefilarete.stalactite.query.Operators;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;

/**
 * Contract for an update operation: allows updating a row of a table with some given column values.
 * 
 * @param <T> table type
 * @author Guillaume Mary
 */
public interface ExecutableUpdate<T extends Table<T>> {
	
	/**
	 * Sets a column value to be updated by this statement.
	 *
	 * @param column any column of targeted {@link Table}
	 * @param value value for the given column
	 * @param <O> value type
	 * @return this
	 */
	<O> ExecutableUpdate<T> set(Column<? extends T, O> column, O value);
	
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
	<O> ExecutableUpdate<T> set(String paramName, O value);
	
	/**
	 * Executes this update statement with given values
	 */
	long execute();
}
