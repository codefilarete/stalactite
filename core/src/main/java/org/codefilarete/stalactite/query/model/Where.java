package org.codefilarete.stalactite.query.model;

import org.codefilarete.tool.collection.Iterables;

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
	
	public Where(Iterable<AbstractCriterion> conditions) {
		if (!Iterables.isEmpty(conditions)) {    // prevents from empty where causing malformed SQL
			add(conditions);
		}
	}
	
	public Where(Object... columns) {
		super(columns);
	}
}
