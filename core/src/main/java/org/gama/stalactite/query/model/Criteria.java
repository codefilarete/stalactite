package org.gama.stalactite.query.model;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.gama.stalactite.persistence.structure.Column;

import static org.gama.stalactite.query.model.AbstractCriterion.LogicalOperator.AND;
import static org.gama.stalactite.query.model.AbstractCriterion.LogicalOperator.OR;

/**
 * A basic implementation of {@link CriteriaChain}.
 * 
 * @author Guillaume Mary
 */
public class Criteria<SELF extends Criteria<SELF>> extends AbstractCriterion implements CriteriaChain<SELF> {
	
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

	protected SELF add(AbstractCriterion condition) {
		this.conditions.add(condition);
		return (SELF) this;
	}

	@Override
	public SELF and(Column column, CharSequence condition) {
		return add(new ColumnCriterion(AND, column, condition));
	}
	
	@Override
	public SELF and(Column column, Operand condition) {
		return add(new ColumnCriterion(AND, column, condition));
	}

	@Override
	public SELF or(Column column, CharSequence condition) {
		return add(new ColumnCriterion(OR, column, condition));
	}
	
	@Override
	public SELF or(Column column, Operand condition) {
		return add(new ColumnCriterion(OR, column, condition));
	}
	
	@Override
	public SELF and(Criteria criteria) {
		criteria.setOperator(AND);
		return add(criteria);
	}

	@Override
	public SELF or(Criteria criteria) {
		criteria.setOperator(OR);
		return add(criteria);
	}
	
	@Override
	public SELF and(Object... columns) {
		return add(new RawCriterion(AND, columns));
	}
	
	@Override
	public SELF or(Object... columns) {
		return add(new RawCriterion(OR, columns));
	}

	@Override
	public Iterator<AbstractCriterion> iterator() {
		return this.conditions.iterator();
	}
}
