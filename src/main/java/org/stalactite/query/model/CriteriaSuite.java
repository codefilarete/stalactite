package org.stalactite.query.model;

import static org.stalactite.query.model.Criteria.LogicalOperator.*;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.stalactite.persistence.structure.Table.Column;

/**
 * @author mary
 */
public abstract class CriteriaSuite<C extends CriteriaSuite> implements Iterable<Object> {
	
	/** Criteria, ClosedCriteria */
	protected List<Object> conditions = new ArrayList<>();

	public CriteriaSuite() {
	}

	public CriteriaSuite(Column column, String condition) {
		add(new Criteria(column, condition));
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

	public C and(ClosedCriteria closedCriteria) {
		closedCriteria.setOperator(And);
		return add(closedCriteria);
	}

	public C or(ClosedCriteria closedCriteria) {
		closedCriteria.setOperator(Or);
		return add(closedCriteria);
	}

	@Override
	public Iterator<Object> iterator() {
		return this.conditions.iterator();
	}
}
