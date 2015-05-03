package org.gama.stalactite.query.model;

/**
 * @author Guillaume Mary
 */
public class AbstractCriterion {
	
	public enum LogicalOperator {
		And,
		Or,
	}
	
	protected LogicalOperator operator;
	
	public AbstractCriterion() {
	}
	
	public AbstractCriterion(LogicalOperator operator) {
		this.operator = operator;
	}
	
	public LogicalOperator getOperator() {
		return operator;
	}
}
