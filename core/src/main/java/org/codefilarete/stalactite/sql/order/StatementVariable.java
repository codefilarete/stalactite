package org.codefilarete.stalactite.sql.order;

import org.codefilarete.stalactite.sql.ddl.structure.Table;

/**
 * Represents a variable that can be applied to an update statement.
 * 
 * @param <V>
 * @param <T>
 * @author Guillaume Mary
 * @see PlaceholderVariable
 * @see ColumnVariable
 */
public abstract class StatementVariable<V, T extends Table<T>> {
	
}