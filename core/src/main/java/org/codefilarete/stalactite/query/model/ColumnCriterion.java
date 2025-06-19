package org.codefilarete.stalactite.query.model;

/**
 * @author Guillaume Mary
 */
public class ColumnCriterion extends AbstractCriterion {
	
	private final Selectable<?> column;
	private final Object /* String or Operator */ condition;
	
	public <O> ColumnCriterion(Selectable<O> column, CharSequence condition) {
		this(null, column, condition);
	}
	
	public <O> ColumnCriterion(Selectable<O> column, ConditionalOperator<? super O, ?> operator) {
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
	
	public ColumnCriterion copyFor(Selectable<?> column) {
		return new ColumnCriterion(getOperator(), column, getCondition());
	}
	
	/**
	 * Implemented for debug. DO NOT RELY ON IT for anything else.
	 */
	@Override
	public String toString() {
		return (operator == null ? "" : operator) + " " + column + " " + condition.getClass().getSimpleName();
	}
}
