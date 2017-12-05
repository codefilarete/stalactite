package org.gama.stalactite.query.model;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.gama.stalactite.persistence.structure.Table.Column;

import static org.gama.stalactite.query.model.AbstractCriterion.LogicalOperator.And;
import static org.gama.stalactite.query.model.AbstractCriterion.LogicalOperator.Or;

/**
 * @author Guillaume Mary
 */
public class Criteria<C extends CriteriaChain<C>> extends AbstractCriterion implements CriteriaChain<C> {
	
	/** Criteria, ClosedCriteria */
	protected List<AbstractCriterion> conditions = new ArrayList<>();

	public Criteria() {
	}

	public Criteria(Column column, String condition) {
		add(new ColumnCriterion(column, condition));
	}
	
	public Criteria(Column column, Operand condition) {
		add(new ColumnCriterion(column, condition));
	}
	
	public Criteria(Object ... columns) {
		add(new RawCriterion(columns));
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

	@Override
	public C and(Column column, CharSequence condition) {
		return add(new ColumnCriterion(And, column, condition));
	}

	public C and(Column column, Operand condition) {
		return add(new ColumnCriterion(And, column, condition));
	}

	@Override
	public C or(Column column, CharSequence condition) {
		return add(new ColumnCriterion(Or, column, condition));
	}
	
	public C or(Column column, Operand condition) {
		return add(new ColumnCriterion(Or, column, condition));
	}
	
	@Override
	public C and(Criteria criteria) {
		criteria.setOperator(And);
		return add(criteria);
	}

	@Override
	public C or(Criteria criteria) {
		criteria.setOperator(Or);
		return add(criteria);
	}
	
	@Override
	public C and(Object... columns) {
		return add(new RawCriterion(And, columns));
	}
	
	@Override
	public C or(Object... columns) {
		return add(new RawCriterion(Or, columns));
	}

	@Override
	public Iterator<AbstractCriterion> iterator() {
		return this.conditions.iterator();
	}
}
