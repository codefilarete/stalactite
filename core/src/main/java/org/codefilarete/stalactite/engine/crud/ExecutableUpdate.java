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
public interface ExecutableUpdate<T extends Table<T>> {
	
	/**
	 * Adds a column to update with its value.
	 *
	 * @param column any column
	 * @param value value for given column
	 * @param <O> value type
	 * @return this
	 */
	<O> ExecutableUpdate<T> set(Column<? extends T, O> column, O value);
	
	/**
	 * Adds a target column which value is took from another column (which can be one of another table if this update is a multi-table one)
	 *
	 * @param column1 any column
	 * @param column2 any column
	 * @param <O> value type
	 * @return this
	 */
	<O> ExecutableUpdate<T> set(Column<? extends T, O> column1, Column<?, O> column2);
	
	/**
	 * Executes this update statement with given values
	 */
	long execute();
	
	/**
	 * Adds a criteria to this update.
	 *
	 * @param column a column target of the condition
	 * @param condition the condition
	 * @return this
	 */
	ExecutableCriteria where(Column<T, ?> column, String condition);
	
	/**
	 * Adds a criteria to this update.
	 *
	 * @param column a column target of the condition
	 * @param condition the condition
	 * @return this
	 */
	ExecutableCriteria where(Column<T, ?> column, ConditionalOperator condition);
}
