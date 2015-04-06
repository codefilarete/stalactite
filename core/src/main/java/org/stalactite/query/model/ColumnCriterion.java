package org.stalactite.query.model;

import org.stalactite.persistence.structure.Table.Column;

/**
 * @author Guillaume Mary
 */
public class ColumnCriterion extends AbstractCriterion {
	
	private final Column column;
	private final String condition;
	
	public ColumnCriterion(Column column, String condition) {
		this(null, column, condition);
	}

	public ColumnCriterion(LogicalOperator operator, Column column, String condition) {
		super(operator);
		this.column = column;
		this.condition = condition;
	}
	
	public Column getColumn() {
		return column;
	}

	public String getCondition() {
		return condition;
	}
	
}
