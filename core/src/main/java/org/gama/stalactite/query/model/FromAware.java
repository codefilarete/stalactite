package org.gama.stalactite.query.model;

import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.query.model.Query.FluentFrom;

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
	
	FluentFrom from(Column leftColumn, Column rightColumn);
	
	FluentFrom from(Column leftColumn, String leftColumnAlias, Column rightColumn, String rightColumnAlias);
	
	FluentFrom fromLeftOuter(Column leftColumn, Column rightColumn);
	
	FluentFrom fromLeftOuter(Column leftColumn, String leftColumnAlias, Column rightColumn, String rightColumnAlias);
	
	FluentFrom fromRightOuter(Column leftColumn, Column rightColumn);
	
	FluentFrom fromRightOuter(Column leftColumn, String leftColumnAlias, Column rightColumn, String rightColumnAlias);
}
