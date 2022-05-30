package org.codefilarete.stalactite.query.model;

import org.codefilarete.stalactite.query.model.Query.FluentFrom;
import org.codefilarete.stalactite.sql.ddl.structure.Table;

/**
 * The interface defining what's possible to do (fluent point of view) after a select
 *
 * @author Guillaume Mary
 */
public interface FromAware {
	
	FluentFrom from(Table leftTable);
	
	FluentFrom from(Table leftTable, String alias);
	
	FluentFrom from(Table leftTable, Table rightTable, String joinCondition);
	
	FluentFrom from(Table leftTable, String leftTableAlias, Table rightTable, String rightTableAlias, String joinCondition);
	
	<I> FluentFrom from(JoinLink<I> leftColumn, JoinLink<I> rightColumn);
}
