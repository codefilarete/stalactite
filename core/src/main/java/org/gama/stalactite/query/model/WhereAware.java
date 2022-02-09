package org.codefilarete.stalactite.query.model;

import org.codefilarete.stalactite.persistence.structure.Column;
import org.codefilarete.stalactite.query.model.Query.FluentWhere;

/**
 * The interface defining what's possible to do (fluent point of view) after a from
 *
 * @author Guillaume Mary
 */
public interface WhereAware extends GroupByAware {
	
	FluentWhere where(Column column, CharSequence condition);
	
	FluentWhere where(Column column, AbstractRelationalOperator condition);
	
	FluentWhere where(Criteria criteria);
	
}
