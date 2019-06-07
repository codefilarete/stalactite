package org.gama.stalactite.query.model;

import org.gama.stalactite.persistence.structure.Column;

/**
 * @author Guillaume Mary
 */
public class Where<C extends Where<C>> extends Criteria<C> {

	public Where() {
	}

	public Where(Column column, String condition) {
		super(column, condition);
	}
	
	public Where(Column column, AbstractOperator condition) {
		super(column, condition);
	}
	
}
