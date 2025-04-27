package org.codefilarete.stalactite.engine.crud;

import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;

/**
 * Contract for an insert operation: allows inserting a row in a table with some given column values.
 * 
 * @param <T> table type
 * @author Guillaume Mary
 */
public interface ExecutableInsert<T extends Table<T>> {
	
	/**
	 * Sets a column value. Overwrites any previous value put for that column.
	 *
	 * @param column any column
	 * @param value value to be inserted
	 * @param <O> value type
	 * @return this
	 */
	<O> ExecutableInsert<T> set(Column<? extends T, O> column, O value);
	
	/**
	 * Executes this insert statement.
	 */
	long execute();
}
