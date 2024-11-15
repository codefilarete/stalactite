package org.codefilarete.stalactite.query.model;

import org.codefilarete.stalactite.sql.ddl.structure.Key;

/**
 * The interface defining what's possible to do (fluent point of view) on a from (more exactly a join to take more general cases)
 * 
 * @author Guillaume Mary
 */
public interface JoinChain<T extends JoinChain<T>> {
	
	<I> T innerJoin(JoinLink<?, I> leftColumn, JoinLink<?, I> rightColumn);
	
	<JOINTYPE> T innerJoin(Key<?, JOINTYPE> leftColumns, Key<?, JOINTYPE> rightColumns);
	
	<I> T leftOuterJoin(JoinLink<?, I> leftColumn, JoinLink<?, I> rightColumn);
	
	<JOINTYPE> T leftOuterJoin(Key<?, JOINTYPE> leftColumns, Key<?, JOINTYPE> rightColumns);
	
	<I> T rightOuterJoin(JoinLink<?, I> leftColumn, JoinLink<?, I> rightColumn);
	
	T innerJoin(Fromable rightTable, String joinClause);
	
	T innerJoin(Fromable rightTable, String rightTableAlias, String joinClause);
	
	T innerJoin(QueryProvider<?> rightTable, String rightTableAlias, String joinClause);
	
	T leftOuterJoin(Fromable rightTable, String joinClause);
	
	T leftOuterJoin(Fromable rightTable, String rightTableAlias, String joinClause);
	
	T leftOuterJoin(QueryProvider<?> rightTable, String rightTableAlias, String joinClause);
	
	T rightOuterJoin(Fromable rightTable, String joinClause);
	
	T rightOuterJoin(Fromable rightTable, String rightTableAlias, String joinClause);
	
	T rightOuterJoin(QueryProvider<?> rightTable, String rightTableAlias, String joinClause);
	
	T crossJoin(Fromable table);
	
	T crossJoin(Fromable table, String tableAlias);
	
	T crossJoin(QueryProvider<?> table, String tableAlias);
	
	/**
	 * Gives an alias for one of the item of the From clause.
	 * Expected to be used after joining elements to make up for the fact that, for any reason, the item couldn't have been given an alias at that time. 
	 * 
	 * @param table the from item to be set an alias
	 * @param alias the alias to set
	 * @return current instance to let one
	 */
	T setAlias(Fromable table, String alias);
}
