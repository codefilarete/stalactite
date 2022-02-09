package org.codefilarete.stalactite.query.model;

import java.util.List;

import org.codefilarete.tool.collection.Arrays;

/**
 * Class aimed at designing criteria that can't be expressed with {@link ColumnCriterion}, for example criteria with function, exists, etc.
 * 
 * @author Guillaume Mary
 */
public class RawCriterion extends AbstractCriterion {
	
	private final List<Object> condition;
	
	public RawCriterion(Object ... condition) {
		super();
		this.condition = Arrays.asList(condition);
	}
	
	public RawCriterion(LogicalOperator operator, Object ... condition) {
		super(operator);
		this.condition = Arrays.asList(condition);
	}
	
	public List<Object> getCondition() {
		return condition;
	}
}
