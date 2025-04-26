package org.codefilarete.stalactite.engine.crud;

import org.codefilarete.stalactite.engine.PersistenceContext.ExecutableCriteria;
import org.codefilarete.stalactite.query.model.ConditionalOperator;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;

/**
 * 
 * @param <T> table type
 * @author Guillaume Mary
 */
public interface ExecutableDelete<T extends Table<T>> {
	
	/**
	 * Executes this delete statement with given values.
	 */
	long execute();
	
	/**
	 * Adds a criteria to this delete.
	 *
	 * @param column a column target of the condition
	 * @param condition the condition
	 * @return this
	 */
	ExecutableCriteria where(Column<T, ?> column, String condition);
	
	/**
	 * Adds a criteria to this delete.
	 *
	 * @param column a column target of the condition
	 * @param condition the condition
	 * @return this
	 */
	ExecutableCriteria where(Column<T, ?> column, ConditionalOperator condition);
}
