package org.gama.stalactite.query.model;

import org.gama.stalactite.persistence.structure.Table.Column;

/**
 * @author Guillaume Mary
 */
public class ColumnCriterion extends AbstractCriterion {
	
	private final Column column;
	private final CharSequence condition;
	
	public ColumnCriterion(Column column, CharSequence condition) {
		this(null, column, condition);
	}

	public ColumnCriterion(LogicalOperator operator, Column column, CharSequence condition) {
		super(operator);
		this.column = column;
		this.condition = condition;
	}
	
	public Column getColumn() {
		return column;
	}

	public CharSequence getCondition() {
		return condition;
	}
	
}
