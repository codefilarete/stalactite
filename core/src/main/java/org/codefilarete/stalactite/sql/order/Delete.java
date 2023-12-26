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
public class Delete {
	
	/** Main table of values to delete */
	private final Table targetTable;
	
	private final Criteria<?> criteriaSurrogate = new Criteria<>();
	
	public Delete(Table targetTable) {
		this.targetTable = targetTable;
	}
	
	public Table getTargetTable() {
		return targetTable;
	}
	
	public CriteriaChain getCriteria() {
		return criteriaSurrogate;
	}
	
	public CriteriaChain where(Column column, String condition) {
		return criteriaSurrogate.and(column, condition);
	}
	
	public CriteriaChain where(Column column, ConditionalOperator condition) {
		return criteriaSurrogate.and(column, condition);
	}
	
}
