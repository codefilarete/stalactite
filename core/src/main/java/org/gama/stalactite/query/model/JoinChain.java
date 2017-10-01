package org.gama.stalactite.query.model;

import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.persistence.structure.Table.Column;

/**
 * @author Guillaume Mary
 */
public interface JoinChain<T extends JoinChain<T>> {
	
	T innerJoin(Column leftColumn, Column rightColumn);
	
	T innerJoin(Column leftColumn, String leftTableAlias, Column rightColumn, String rightTableAlias);
	
	T leftOuterJoin(Column leftColumn, Column rightColumn);
	
	T leftOuterJoin(Column leftColumn, String leftTableAlias, Column rightColumn, String rightTableAlias);
	
	T rightOuterJoin(Column leftColumn, Column rightColumn);
	
	T rightOuterJoin(Column leftColumn, String leftTableAlias, Column rightColumn, String rightTableAlias);
	
	T innerJoin(Table leftTable, Table rightTable, String joinClause);
	
	T innerJoin(Table leftTable, String leftTableAlias, Table rigTable, String rightTableAlias, String joinClause);
	
	T leftOuterJoin(Table leftTable, Table rigTable, String joinClause);
	
	T leftOuterJoin(Table leftTable, String leftTableAlias, Table rigTable, String rightTableAlias, String joinClause);
	
	T rightOuterJoin(Table leftTable, Table rigTable, String joinClause);
	
	T rightOuterJoin(Table leftTable, String leftTableAlias, Table rigTable, String rightTableAlias, String joinClause);
	
	T crossJoin(Table table);
	
	T crossJoin(Table table, String tableAlias);
}
