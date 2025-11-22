package org.codefilarete.stalactite.dsl.entity;

import java.util.Collection;

/**
 * Interface for marking a mappedBy(..) as mandatory.
 *
 * @param <C> entity type
 * @param <I> entity identifier type
 * @param <O> type of {@link Collection} element
 * @param <S> refined {@link Collection} type
 *
 * @author Guillaume Mary
 */
public interface FluentMappingBuilderOneToManyMappedByOptions<C, I, O, S extends Collection<O>> extends FluentMappingBuilderOneToManyOptions<C, I, O, S> {
	
	/**
	 * Mark this mapped-by relation as mandatory which set the database column as not nullable.
	 * @return the global configurer
	 */
	FluentMappingBuilderOneToManyMappedByOptions<C, I, O, S> mandatory();
}
