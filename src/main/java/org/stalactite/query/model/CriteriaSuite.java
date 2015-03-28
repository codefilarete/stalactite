package org.stalactite.query.model;

import static org.stalactite.query.model.AbstractCriterion.LogicalOperator.And;
import static org.stalactite.query.model.AbstractCriterion.LogicalOperator.Or;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.stalactite.persistence.structure.Table.Column;

/**
 * @author Guillaume Mary
 */
public class CriteriaSuite<C extends CriteriaSuite> extends AbstractCriterion implements Iterable<AbstractCriterion> {
	
	/** Criteria, ClosedCriteria */
	protected List<AbstractCriterion> conditions = new ArrayList<>();

	public CriteriaSuite() {
	}

	public CriteriaSuite(Column column, String condition) {
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

	public C and(CriteriaSuite criteriaSuite) {
		criteriaSuite.setOperator(And);
		return add(criteriaSuite);
	}

	public C or(CriteriaSuite criteriaSuite) {
		criteriaSuite.setOperator(Or);
		return add(criteriaSuite);
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
