package org.stalactite.query.model;

import org.stalactite.persistence.structure.Table.Column;
import org.stalactite.query.model.Criteria.LogicalOperator;

/**
 * Classe marqueuse pour identifier les conditions entre parenthèses.
 * 
 * @author mary
 */
public class ClosedCriteria<C extends ClosedCriteria> extends CriteriaSuite<C> {
	
	private LogicalOperator operator;
	
	public ClosedCriteria(Column column, String condition) {
		super(column, condition);
	}

	public LogicalOperator getOperator() {
		return operator;
	}

	protected void setOperator(LogicalOperator operator) {
		this.operator = operator;
	}
	
	@Override
	public C and(Column column, String condition) {
		return super.and(column, condition);
	}

	@Override
	public C or(Column column, String condition) {
		return super.or(column, condition);
	}

	@Override
	public C and(ClosedCriteria closedCriteria) {
		return super.and(closedCriteria);
	}

	@Override
	public C or(ClosedCriteria closedCriteria) {
		return super.or(closedCriteria);
	}
}
