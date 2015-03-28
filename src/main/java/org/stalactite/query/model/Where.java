package org.stalactite.query.model;

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
	public C and(ClosedCriteria closedCriteria) {
		return super.and(closedCriteria);
	}

	@Override
	public C or(ClosedCriteria closedCriteria) {
		return super.or(closedCriteria);
	}
}
