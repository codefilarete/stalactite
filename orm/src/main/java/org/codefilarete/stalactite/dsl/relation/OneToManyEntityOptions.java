package org.codefilarete.stalactite.dsl.relation;

import java.util.Collection;

import org.codefilarete.stalactite.engine.configurer.onetomany.OneToManyRelationConfigurer;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.danekja.java.util.function.serializable.SerializableBiConsumer;
import org.danekja.java.util.function.serializable.SerializableFunction;

/**
 * @author Guillaume Mary
 */
public interface OneToManyEntityOptions<C, I, O, S extends Collection<O>> extends OneToManyOptions<C, O, S> {
	
	/**
	 * Defines reverse side owning column.
	 * Note that defining it this way will not allow relation to be fixed in memory (after select in database), prefer {@link #mappedBy(SerializableBiConsumer)}.
	 * Use this method to define unidirectional relation.
	 *
	 * If the relation is already defined through {@link #mappedBy(SerializableFunction)} or {@link #mappedBy(SerializableBiConsumer)} then there's no
	 * guaranty about which one will be taken first. Algorithm is defined in {@link OneToManyRelationConfigurer}.
	 * 
	 * @param reverseLink opposite owner of the relation
	 * @return the global mapping configurer
	 */
	OneToManyEntityOptions<C, I, O, S> mappedBy(Column<?, I> reverseLink);
	
	/**
	 * Activates entity order persistence and indicates {@link Column} to be used for it.
	 * Collection that stores data is expected to support ordering by index (as List or LinkedHashSet)
	 *
	 * @param orderingColumn the column to be used for order persistence
	 * @return the global mapping configurer
	 */
	OneToManyEntityOptions<C, I, O, S> indexedBy(Column<?, Integer> orderingColumn);
	
}