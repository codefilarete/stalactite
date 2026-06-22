package org.codefilarete.stalactite.dsl.relation;

import java.util.Collection;

import org.codefilarete.stalactite.sql.ddl.structure.Column;

/**
 * Interface for many-to-one relation options of entities (bean with identifier)
 *
 * @param <C> entity type
 * @param <O> target entity type
 * @param <S> reverse {@link Collection} type
 *
 * @author Guillaume Mary
 */
public interface ManyToOneEntityOptions<C, O, S extends Collection<C>> extends ManyToOneOptions<C, O, S> {

	/**
	 * Defines the foreign key column referencing the target entity key.
	 * This is the typed equivalent of {@link #columnName(String)} and is valuable only for single key cases.
	 *
	 * @param reverseLink foreign key column referencing the target entity key
	 * @return the global mapping configurer
	 */
	ManyToOneEntityOptions<C, O, S> mappedBy(Column<?, ?> reverseLink);
}
