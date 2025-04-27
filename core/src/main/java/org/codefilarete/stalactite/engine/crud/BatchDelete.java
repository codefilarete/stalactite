package org.codefilarete.stalactite.engine.crud;

import org.codefilarete.stalactite.sql.ddl.structure.Table;

public interface BatchDelete<T extends Table<T>> extends ExecutableDelete<T> {
	
	/**
	 * Overridden to adapt return type
	 */
	<O> BatchDelete<T> set(String paramName, O value);
	
	/**
	 * Open a new row for deletion.
	 * 
	 * @return this
	 */
	BatchDelete<T> newRow();
}
