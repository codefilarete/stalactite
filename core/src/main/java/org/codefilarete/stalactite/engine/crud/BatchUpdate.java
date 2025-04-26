package org.codefilarete.stalactite.engine.crud;

import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;

/**
 * 
 * @param <T> table type
 * @author Guillaume Mary
 */
public interface BatchUpdate<T extends Table<T>> extends ExecutableUpdate<T> {
	
	/**
	 * Overridden to adapt return type
	 */
	<C> BatchUpdate<T> set(Column<? extends T, C> column, C value);
	
	<C> BatchUpdate<T> set(String argName, C value);
	
	/**
	 * Open a new row for insertion. Must be chained with {@link #set(Column, Object)} to fill it.
	 *
	 * @return this
	 */
	BatchUpdate<T> newRow();
	
	/**
	 * Executes this insert statement and insert all the registered rows.
	 */
	long execute();
}
