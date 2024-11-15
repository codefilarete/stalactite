package org.codefilarete.stalactite.query.model;

/**
 * @author Guillaume Mary
 */
public class Where<SELF extends Where<SELF>> extends Criteria<SELF> {

	public Where() {
	}

	public Where(Selectable<?> column, String condition) {
		super(column, condition);
	}
	
	public <O> Where(Selectable<O> column, ConditionalOperator<? super O, ?> condition) {
		super(column, condition);
	}
	
	public Where(Object... columns) {
		super(columns);
	}
}
