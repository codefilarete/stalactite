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
	<O> BatchUpdate<T> set(Column<? extends T, O> column, O value);
	
	/**
	 * Overridden to adapt return type
	 */
	<O> BatchUpdate<T> set(String paramName, O value);
	
	/**
	 * Open a new row for insertion. Must be chained with {@link #set(Column, Object)} to fill it.
	 *
	 * @return this
	 */
	BatchUpdate<T> newRow();
}
