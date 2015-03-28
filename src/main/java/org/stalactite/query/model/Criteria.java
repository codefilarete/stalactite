package org.stalactite.query.model;

import org.stalactite.persistence.structure.Table.Column;

/**
 * @author mary
 */
public class Criteria {

	public enum LogicalOperator {
		And,
		Or,
	}

	private final Column column;
	private final String condition;
	private final LogicalOperator operator;

	public Criteria(Column column, String condition) {
		this(null, column, condition);
	}

	public Criteria(LogicalOperator operator, Column column, String condition) {
		this.operator = operator;
		this.column = column;
		this.condition = condition;
	}
	
	public Column getColumn() {
		return column;
	}

	public String getCondition() {
		return condition;
	}

	public LogicalOperator getOperator() {
		return operator;
	}
}
