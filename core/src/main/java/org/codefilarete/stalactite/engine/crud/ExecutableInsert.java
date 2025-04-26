package org.codefilarete.stalactite.engine.crud;

import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;

/**
 * 
 * @param <T> table type
 * @author Guillaume Mary
 */
public interface ExecutableInsert<T extends Table<T>> {
	
	/**
	 * Adds a column to set and its value. Overwrites any previous value put for that column.
	 *
	 * @param column any column
	 * @param value value to be inserted
	 * @param <C> value type
	 * @return this
	 */
	<C> ExecutableInsert<T> set(Column<? extends T, C> column, C value);
	
	/**
	 * Executes this insert statement.
	 */
	long execute();
}
