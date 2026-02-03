package org.codefilarete.stalactite.dsl.relation;

import java.util.Collection;

/**
 * Interface to add some options for a one-to-many relationship with a join table.
 *
 * @param <C> entity type
 * @param <O> type of {@link Collection} element
 * @param <S1> source {@link Collection} type
 * @param <S2> reverse {@link Collection} type
 *
 * @author Guillaume Mary
 */
public interface ManyToManyJoinTableOptions<C, O, S1 extends Collection<O>, S2 extends Collection<C>> extends ManyToManyEntityOptions<C, O, S1, S2> {
	
	/**
	 * Sets the column name in the join table that corresponds to the owning-entity's identifier.
	 * 
	 * @param columnName the column name
	 * @return an enhanced version of 'this' to make it fluent with other options
	 */
	ManyToManyJoinTableOptions<C, O, S1, S2> sourceJoinColumn(String columnName);
	
	/**
	 * Sets the column name in the join table that corresponds to the mapped-entity's identifier.
	 * 
	 * @param columnName the column name
	 * @return an enhanced version of 'this' to make it fluent with other options
	 */
	ManyToManyJoinTableOptions<C, O, S1, S2> targetJoinColumn(String columnName);
}
