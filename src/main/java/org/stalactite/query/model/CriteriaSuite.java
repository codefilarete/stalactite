package org.stalactite.query.model;

import static org.stalactite.query.model.Criteria.LogicalOperator.*;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.stalactite.persistence.structure.Table.Column;
import org.stalactite.query.model.Criteria.LogicalOperator;

/**
 * @author mary
 */
public class CriteriaSuite<C extends CriteriaSuite> implements Iterable<Object> {
	
	private LogicalOperator operator;
	
	/** Criteria, ClosedCriteria */
	protected List<Object> conditions = new ArrayList<>();

	public CriteriaSuite() {
	}

	public CriteriaSuite(Column column, String condition) {
		add(new Criteria(column, condition));
	}

	public LogicalOperator getOperator() {
		return operator;
	}

	protected void setOperator(LogicalOperator operator) {
		this.operator = operator;
	}
	
	public List<Object> getConditions() {
		return conditions;
	}

	protected C add(Object condition) {
		this.conditions.add(condition);
		return (C) this;
	}

	public C and(Column column, String condition) {
		return add(new Criteria(And, column, condition));
	}

	public C or(Column column, String condition) {
		return add(new Criteria(Or, column, condition));
	}

	public C and(CriteriaSuite criteriaSuite) {
		criteriaSuite.setOperator(And);
		return add(criteriaSuite);
	}

	public C or(CriteriaSuite criteriaSuite) {
		criteriaSuite.setOperator(Or);
		return add(criteriaSuite);
	}

	@Override
	public Iterator<Object> iterator() {
		return this.conditions.iterator();
	}
}
