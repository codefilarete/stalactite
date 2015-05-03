package org.gama.stalactite.query.model;

import static org.gama.stalactite.query.model.AbstractCriterion.LogicalOperator.And;
import static org.gama.stalactite.query.model.AbstractCriterion.LogicalOperator.Or;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.gama.stalactite.persistence.structure.Table.Column;

/**
 * @author Guillaume Mary
 */
public class Criteria<C extends Criteria> extends AbstractCriterion implements Iterable<AbstractCriterion> {
	
	/** Criteria, ClosedCriteria */
	protected List<AbstractCriterion> conditions = new ArrayList<>();

	public Criteria() {
	}

	public Criteria(Column column, String condition) {
		add(new ColumnCriterion(column, condition));
	}

	protected void setOperator(LogicalOperator operator) {
		this.operator = operator;
	}
	
	public List<AbstractCriterion> getConditions() {
		return conditions;
	}

	protected C add(AbstractCriterion condition) {
		this.conditions.add(condition);
		return (C) this;
	}

	public C and(Column column, String condition) {
		return add(new ColumnCriterion(And, column, condition));
	}

	public C or(Column column, String condition) {
		return add(new ColumnCriterion(Or, column, condition));
	}

	public C and(Criteria criteria) {
		criteria.setOperator(And);
		return add(criteria);
	}

	public C or(Criteria criteria) {
		criteria.setOperator(Or);
		return add(criteria);
	}
	
	public C and(Object ... columns) {
		return add(new RawCriterion(And, columns));
	}

	public C or(Object ... columns) {
		return add(new RawCriterion(Or, columns));
	}

	@Override
	public Iterator<AbstractCriterion> iterator() {
		return this.conditions.iterator();
	}
}
