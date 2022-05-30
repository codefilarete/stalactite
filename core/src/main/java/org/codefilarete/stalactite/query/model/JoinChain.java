package org.codefilarete.stalactite.query.model;

/**
 * The interface defining what's possible to do (fluent point of view) on a from (more exactly a join to take more general cases)
 * 
 * @author Guillaume Mary
 */
public interface JoinChain<T extends JoinChain<T>> {
	
	<I> T innerJoin(JoinLink<I> leftColumn, JoinLink<I> rightColumn);
	
	<I> T leftOuterJoin(JoinLink<I> leftColumn, JoinLink<I> rightColumn);
	
	<I> T rightOuterJoin(JoinLink<I> leftColumn, JoinLink<I> rightColumn);
	
	T innerJoin(Fromable leftTable, Fromable rightTable, String joinClause);
	
	T innerJoin(Fromable leftTable, String leftTableAlias, Fromable rigTable, String rightTableAlias, String joinClause);
	
	T leftOuterJoin(Fromable leftTable, Fromable rigTable, String joinClause);
	
	T leftOuterJoin(Fromable leftTable, String leftTableAlias, Fromable rigTable, String rightTableAlias, String joinClause);
	
	T rightOuterJoin(Fromable leftTable, Fromable rigTable, String joinClause);
	
	T rightOuterJoin(Fromable leftTable, String leftTableAlias, Fromable rigTable, String rightTableAlias, String joinClause);
	
	T crossJoin(Fromable table);
	
	T crossJoin(Fromable table, String tableAlias);
	
	T setAlias(Fromable table, String alias);
}
