package org.codefilarete.stalactite.dsl.relation;

import java.util.Collection;

import org.codefilarete.stalactite.engine.configurer.onetomany.OneToManyRelationConfigurer;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.danekja.java.util.function.serializable.SerializableBiConsumer;
import org.danekja.java.util.function.serializable.SerializableFunction;

/**
 * Interface for one-to-many relation options of entities (bean with identifier)
 *
 * @param <C> entity type
 * @param <I> entity identifier type
 * @param <O> type of {@link Collection} element
 * @param <S> refined {@link Collection} type
 *
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
	 * Activates entity order persistence and indicates the column name to be used for it.
	 * The collection type that stores data is expected to support ordering by index (as List or LinkedHashSet)
	 *
	 * @param orderingColumn the column to be used for order persistence
	 * @return the global mapping configurer
	 */
	OneToManyEntityOptions<C, I, O, S> indexedBy(Column<?, Integer> orderingColumn);
	
	/**
	 * Defines the table name of the association table.
	 * This is not compatible with defining {@link #reverseJoinColumn(String)} because it would lead to ambiguity, hence
	 * there's no guarantee about which one will be taken first. Algorithm is defined in {@link OneToManyRelationConfigurer}.
	 * 
	 * Note that we don't define it for embeddable types (meaning putting this method in {@link OneToManyOptions})
	 * because fixing the association table name for a reusable configuration means that all data will be stored in
	 * the same table which will cause name collision on foreign keys.
	 * 
	 * @param tableName the table name of the association table
	 * @return the global mapping configurer
	 */
	OneToManyJoinTableOptions<C, O, S> joinTable(String tableName);
}
