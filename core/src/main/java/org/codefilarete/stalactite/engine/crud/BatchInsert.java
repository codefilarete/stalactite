package org.codefilarete.stalactite.engine.crud;

import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;

/**
 * 
 * @param <T> table type
 * @author Guillaume Mary
 */
public interface BatchInsert<T extends Table<T>> extends ExecutableInsert<T> {
	
	/**
	 * Adds a column to set and its value. Overwrites any previous value put for that column.
	 *
	 * @param column any column
	 * @param value value to be inserted
	 * @param <C> value type
	 * @return this
	 */
	<C> BatchInsert<T> set(Column<? extends T, C> column, C value);
	
	/**
	 * Open a new row for insertion. Must be chained with {@link #set(Column, Object)} to fill it.
	 *
	 * @return this
	 */
	BatchInsert<T> newRow();
	
	/**
	 * Executes this insert statement and insert all the registered rows.
	 */
	long execute();
}
