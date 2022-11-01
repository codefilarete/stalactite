package org.codefilarete.stalactite.query.model;

import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.query.model.Query.FluentWhereClause;

/**
 * The interface defining what's possible to do (fluent point of view) after a from
 *
 * @author Guillaume Mary
 */
public interface WhereAware extends GroupByAware {
	
	<O> FluentWhereClause where(Column<?, O> column, CharSequence condition);
	
	<O> FluentWhereClause where(Column<?, O> column, ConditionalOperator<? super O> condition);
	
	FluentWhereClause where(Criteria criteria);
	
}
