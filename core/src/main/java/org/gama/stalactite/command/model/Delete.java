package org.gama.stalactite.command.model;

import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.query.model.Criteria;
import org.gama.stalactite.query.model.CriteriaChain;
import org.gama.stalactite.query.model.Operand;

/**
 * A simple representation of a SQL delete clause, and a way to build it easily/fluently.
 * 
 * @author Guillaume Mary
 * @see org.gama.stalactite.command.builder.DeleteCommandBuilder
 */
public class Delete {
	
	/** Target of the values to insert */
	private final Table targetTable;
	
	private final Criteria criteriaSurrogate = new Criteria();
	
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
	
	public CriteriaChain where(Column column, Operand condition) {
		return criteriaSurrogate.and(column, condition);
	}
	
}
