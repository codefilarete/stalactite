package org.stalactite.query.model;

import java.util.List;

import org.stalactite.lang.collection.Arrays;

/**
 * Classe pour modéliser les critères non exprimable par {@link ColumnCriterion}, par exemple des critères avec fonction,
 * exists, etc.
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
