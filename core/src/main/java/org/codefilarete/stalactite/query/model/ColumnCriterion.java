package org.codefilarete.stalactite.query.model;

import org.codefilarete.stalactite.sql.ddl.structure.Column;

/**
 * @author Guillaume Mary
 */
public class ColumnCriterion extends AbstractCriterion {
	
	private final Selectable column;
	private final Object /* String or Operator */ condition;
	
	public <O> ColumnCriterion(Column<?, O> column, CharSequence condition) {
		this(null, column, condition);
	}
	
	public <O> ColumnCriterion(Column<?, O> column, ConditionalOperator<? super O, ?> operator) {
		this(null, column, operator);
	}
	
	public ColumnCriterion(LogicalOperator operator, Selectable column, Object /* String or Operator */ condition) {
		super(operator);
		this.column = column;
		this.condition = condition;
	}
	
	public Selectable getColumn() {
		return column;
	}

	public Object getCondition() {
		return condition;
	}
	
	public ColumnCriterion copyFor(Selectable column) {
		return new ColumnCriterion(getOperator(), column, getCondition());
	}
}
