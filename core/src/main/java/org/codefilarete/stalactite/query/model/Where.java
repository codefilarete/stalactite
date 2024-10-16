package org.codefilarete.stalactite.query.model;

import org.codefilarete.stalactite.sql.ddl.structure.Column;

/**
 * @author Guillaume Mary
 */
public class Where<SELF extends Where<SELF>> extends Criteria<SELF> {

	public Where() {
	}

	public Where(Column column, String condition) {
		super(column, condition);
	}
	
	public Where(Column column, ConditionalOperator condition) {
		super(column, condition);
	}
	
	public Where(Object... columns) {
		super(columns);
	}
}
