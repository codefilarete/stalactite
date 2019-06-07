package org.gama.stalactite.persistence.query;

import javax.annotation.Nullable;

import org.gama.reflection.MemberDefinition;
import org.gama.reflection.ValueAccessPointByMethodReference;

/**
 * @author Guillaume Mary
 */
public class ValueAccessPointCriterion {
	
	public enum LogicalOperator {
		AND,
		OR
	}
	
	@Nullable
	private final LogicalOperator operator;
	
	private final ValueAccessPointByMethodReference valueAccessPoint;
	private final Object /* String or RelationalOperator */ condition;
	public ValueAccessPointCriterion(@Nullable LogicalOperator operator, ValueAccessPointByMethodReference valueAccessPoint, Object condition) {
		this.operator = operator;
		this.valueAccessPoint = valueAccessPoint;
		this.condition = condition;
	}
	
	@Nullable
	public LogicalOperator getOperator() {
		return operator;
	}
	
	public ValueAccessPointByMethodReference getValueAccessPoint() {
		return valueAccessPoint;
	}
	
	public Object getCondition() {
		return condition;
	}
	
	@Override
	public String toString() {
		return operator +
				" " + MemberDefinition.toString(valueAccessPoint) +
				" = " + condition;
	}
}
