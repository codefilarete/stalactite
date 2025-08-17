package org.codefilarete.stalactite.query.model;

import org.codefilarete.stalactite.sql.ddl.structure.Column;

import static org.codefilarete.stalactite.query.model.LogicalOperator.*;

/**
 * @author Guillaume Mary
 */
public interface CriteriaChain<SELF extends CriteriaChain<SELF>> extends Iterable<AbstractCriterion> {
	
	default SELF and(Column column, CharSequence condition) {
		return add(AND, column, condition);
	}
	
	default SELF or(Column column, CharSequence condition) {
		return add(OR, column, condition);
	}
	
	default SELF and(Column column, ConditionalOperator condition) {
		return add(AND, column, condition);
	}
	
	default SELF or(Column column, ConditionalOperator condition) {
		return add(OR, column, condition);
	}
	
	default SELF and(CriteriaChain<?> criteria) {
		return add(AND, criteria);
	}
	
	default SELF or(CriteriaChain<?> criteria) {
		return add(OR, criteria);
	}
	
	default SELF and(Object... columns) {
		return add(AND, columns);
	}
	
	default SELF or(Object... columns) {
		return add(OR, columns);
	}
	
	SELF add(LogicalOperator logicalOperator, Selectable<?> column, CharSequence condition);
	
	SELF add(LogicalOperator logicalOperator, Selectable<?> column, ConditionalOperator<?, ?> condition);
	
	SELF add(LogicalOperator logicalOperator, CriteriaChain<?> criteria);
	
	SELF add(LogicalOperator logicalOperator, Object... columns);
	
	SELF add(AbstractCriterion condition);
	
	/**
	 * Adds the given criteria to this chain by simply appending it to the end of the chain.
	 * @param criteria the condition to be added to this chain
	 * @return this
	 */
	SELF add(Iterable<AbstractCriterion> criteria);
}
