package org.codefilarete.stalactite.dsl.entity;

import org.codefilarete.stalactite.dsl.relation.OneToManyJoinTableOptions;

import java.util.Collection;

/**
 * Interface to add some options for a one-to-many relationship with a join table.
 *
 * @param <C> entity type
 * @param <I> entity identifier type
 * @param <O> type of {@link Collection} element
 * @param <S> refined {@link Collection} type
 *
 * @author Guillaume Mary
 */
public interface FluentMappingBuilderOneToManyJoinTableOptions<C, I, O, S extends Collection<O>> extends FluentMappingBuilderOneToManyOptions<C, I, O, S>, OneToManyJoinTableOptions<C, O, S> {
	
	/**
	 * Sets the column name in the join table that corresponds to the owning-entity's identifier.
	 * 
	 * @param columnName the column name
	 * @return an enhanced version of 'this' to make it fluent with other options
	 */
	FluentMappingBuilderOneToManyJoinTableOptions<C, I, O, S> sourceJoinColumn(String columnName);
	
	/**
	 * Sets the column name in the join table that corresponds to the mapped-entity's identifier.
	 * 
	 * @param columnName the column name
	 * @return an enhanced version of 'this' to make it fluent with other options
	 */
	FluentMappingBuilderOneToManyJoinTableOptions<C, I, O, S> targetJoinColumn(String columnName);
}
