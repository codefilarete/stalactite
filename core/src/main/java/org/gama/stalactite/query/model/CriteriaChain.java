package org.gama.stalactite.query.model;

import org.gama.stalactite.persistence.structure.Table.Column;

/**
 * @author Guillaume Mary
 */
public interface CriteriaChain<SELF extends CriteriaChain<SELF>> extends Iterable<AbstractCriterion> {
	
	SELF and(Column column, String condition);
	
	SELF or(Column column, String condition);
	
	SELF and(Criteria criteria);
	
	SELF or(Criteria criteria);
	
	SELF and(Object... columns);
	
	SELF or(Object... columns);
}
