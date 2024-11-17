package org.codefilarete.stalactite.sql.order;

import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.query.model.ConditionalOperator;
import org.codefilarete.stalactite.query.model.Criteria;
import org.codefilarete.stalactite.query.model.CriteriaChain;

/**
 * A fluent way of writing a SQL delete clause by leveraging {@link Column} : conditions can only be set through it.
 * It handles multi table deletion by allowing to add columns coming from different tables, but it is up to caller to join them correctly as
 * he would do for standard SQL, because no check is done by this class nor by {@link DeleteCommandBuilder}
 * 
 * @author Guillaume Mary
 * @see DeleteCommandBuilder
 */
public class Delete<T extends Table<T>> {
	
	/** Main table of values to delete */
	private final T targetTable;
	
	private final Criteria<?> criteriaSurrogate = new Criteria<>();
	
	public Delete(T targetTable) {
		this.targetTable = targetTable;
	}
	
	public T getTargetTable() {
		return targetTable;
	}
	
	public CriteriaChain getCriteria() {
		return criteriaSurrogate;
	}
	
	/**
	 * Adds a criteria to this delete.
	 *
	 * @param column a column target of the condition
	 * @param condition the condition
	 * @return this
	 */
	public CriteriaChain where(Column<T, ?> column, String condition) {
		return criteriaSurrogate.and(column, condition);
	}
	
	/**
	 * Adds a criteria to this delete.
	 *
	 * @param column a column target of the condition
	 * @param condition the condition
	 * @return this
	 */
	public CriteriaChain where(Column<T, ?> column, ConditionalOperator condition) {
		return criteriaSurrogate.and(column, condition);
	}
	
}
