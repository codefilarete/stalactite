package org.gama.stalactite.query.model;

import static org.gama.stalactite.query.model.AbstractCriterion.LogicalOperator.And;
import static org.gama.stalactite.query.model.AbstractCriterion.LogicalOperator.Or;

import org.gama.stalactite.persistence.structure.Table.Column;

/**
 * @author mary
 */
public class Where<C extends Where> extends Criteria<C> {

	public Where() {
	}

	public Where(Column column, String condition) {
		super(column, condition);
	}
	
	@Override
	public C and(Column column, String condition) {
		return super.and(column, condition);
	}

	@Override
	public C or(Column column, String condition) {
		return super.or(column, condition);
	}

	@Override
	public C and(Criteria criteria) {
		return super.and(criteria);
	}

	@Override
	public C or(Criteria criteria) {
		return super.or(criteria);
	}
	
	@Override
	public C and(Object ... columns) {
		return add(new RawCriterion(And, columns));
	}

	@Override
	public C or(Object ... columns) {
		return add(new RawCriterion(Or, columns));
	}
}
