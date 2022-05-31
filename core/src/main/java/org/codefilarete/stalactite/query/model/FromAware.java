package org.codefilarete.stalactite.query.model;

import org.codefilarete.stalactite.query.model.Query.FluentFrom;

/**
 * The interface defining what's possible to do (fluent point of view) after a select
 *
 * @author Guillaume Mary
 */
public interface FromAware {
	
	FluentFrom from(Fromable leftTable);
	
	FluentFrom from(Fromable leftTable, String alias);
	
	FluentFrom from(Fromable leftTable, Fromable rightTable, String joinCondition);
	
	FluentFrom from(Fromable leftTable, String leftTableAlias, Fromable rightTable, String rightTableAlias, String joinCondition);
	
	<I> FluentFrom from(JoinLink<I> leftColumn, JoinLink<I> rightColumn);
}
