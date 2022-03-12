package org.codefilarete.stalactite.query.model;

/**
 * @author Guillaume Mary
 */
public abstract class AbstractCriterion {
	
	public enum LogicalOperator {
		AND,
		OR,
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
