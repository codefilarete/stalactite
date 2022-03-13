package org.codefilarete.stalactite.persistence.sql.order;

import org.codefilarete.stalactite.persistence.structure.Column;
import org.codefilarete.stalactite.persistence.structure.Table;
import org.codefilarete.stalactite.query.model.AbstractRelationalOperator;
import org.codefilarete.stalactite.query.model.Criteria;
import org.codefilarete.stalactite.query.model.CriteriaChain;

/**
 * A simple representation of a SQL delete clause, and a way to build it easily/fluently.
 * 
 * @author Guillaume Mary
 * @see DeleteCommandBuilder
 */
public class Delete<T extends Table> {
	
	/** Target of the values to insert */
	private final T targetTable;
	
	private final Criteria criteriaSurrogate = new Criteria();
	
	public Delete(T targetTable) {
		this.targetTable = targetTable;
	}
	
	public T getTargetTable() {
		return targetTable;
	}
	
	public CriteriaChain getCriteria() {
		return criteriaSurrogate;
	}
	
	public CriteriaChain where(Column column, String condition) {
		return criteriaSurrogate.and(column, condition);
	}
	
	public CriteriaChain where(Column column, AbstractRelationalOperator condition) {
		return criteriaSurrogate.and(column, condition);
	}
	
}
