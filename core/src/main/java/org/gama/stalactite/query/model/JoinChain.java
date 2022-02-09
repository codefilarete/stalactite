package org.codefilarete.stalactite.query.model;

import org.codefilarete.stalactite.persistence.structure.Table;
import org.codefilarete.stalactite.persistence.structure.Column;

/**
 * The interface defining what's possible to do (fluent point of view) on a from (more exactly a join to take more general cases)
 * 
 * @author Guillaume Mary
 */
public interface JoinChain<T extends JoinChain<T>> {
	
	T innerJoin(Column leftColumn, Column rightColumn);
	
	T leftOuterJoin(Column leftColumn, Column rightColumn);
	
	T rightOuterJoin(Column leftColumn, Column rightColumn);
	
	T innerJoin(Table leftTable, Table rightTable, String joinClause);
	
	T innerJoin(Table leftTable, String leftTableAlias, Table rigTable, String rightTableAlias, String joinClause);
	
	T leftOuterJoin(Table leftTable, Table rigTable, String joinClause);
	
	T leftOuterJoin(Table leftTable, String leftTableAlias, Table rigTable, String rightTableAlias, String joinClause);
	
	T rightOuterJoin(Table leftTable, Table rigTable, String joinClause);
	
	T rightOuterJoin(Table leftTable, String leftTableAlias, Table rigTable, String rightTableAlias, String joinClause);
	
	T crossJoin(Table table);
	
	T crossJoin(Table table, String tableAlias);
	
	T setAlias(Table table, String alias);
}
