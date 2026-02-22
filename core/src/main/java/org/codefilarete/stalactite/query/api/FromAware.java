package org.codefilarete.stalactite.query.api;

import org.codefilarete.stalactite.query.model.Query.FluentFromClause;

/**
 * The interface defining what's possible to do (fluent point of view) after a select
 *
 * @author Guillaume Mary
 */
public interface FromAware {
	
	FluentFromClause from(Fromable leftTable);
	
	FluentFromClause from(QueryProvider<?> query, String alias);
	
	FluentFromClause from(Fromable leftTable, String alias);
	
	FluentFromClause from(Fromable leftTable, Fromable rightTable, String joinCondition);
	
	FluentFromClause from(Fromable leftTable, String leftTableAlias, Fromable rightTable, String rightTableAlias, String joinCondition);
	
	<I> FluentFromClause from(JoinLink<?, I> leftColumn, JoinLink<?, I> rightColumn);
}
