package org.gama.stalactite.query.model;

import org.gama.stalactite.persistence.structure.Table.Column;

/**
 * @author Guillaume Mary
 */
public class ColumnCriterion extends AbstractCriterion {
	
	private final Column column;
	private final Object /* String or Operand */ condition;
	
	public ColumnCriterion(Column column, String condition) {
		this(null, column, condition);
	}
	
	public ColumnCriterion(Column column, Operand operand) {
		this(null, column, operand);
	}
	
	public ColumnCriterion(LogicalOperator operator, Column column, Object /* String or Operand */ condition) {
		super(operator);
		this.column = column;
		this.condition = condition;
	}
	
	public Column getColumn() {
		return column;
	}

	public Object getCondition() {
		return condition;
	}
	
}
