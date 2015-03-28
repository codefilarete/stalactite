package org.stalactite.query.model;

import static org.stalactite.query.model.AbstractCriterion.LogicalOperator.And;
import static org.stalactite.query.model.AbstractCriterion.LogicalOperator.Or;

import org.stalactite.persistence.structure.Table.Column;

/**
 * @author mary
 */
public class Where<C extends Where> extends CriteriaSuite<C> {

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
	public C and(CriteriaSuite criteriaSuite) {
		return super.and(criteriaSuite);
	}

	@Override
	public C or(CriteriaSuite criteriaSuite) {
		return super.or(criteriaSuite);
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
