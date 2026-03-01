package org.codefilarete.stalactite.dsl.relation;

import org.codefilarete.reflection.SerializableAccessor;
import org.codefilarete.reflection.SerializableMutator;
import org.codefilarete.stalactite.dsl.property.CascadeOptions;
import org.codefilarete.stalactite.engine.configurer.onetoone.OneToOneRelationConfigurer;

/**
 * @param <C> entity type
 * @param <O> target entity type
 * @author Guillaume Mary
 */
public interface OneToOneOptions<C, O> extends CascadeOptions {
	
	/** Marks the relation as mandatory. Hence joins will be inner ones and a checking for non null value will be done before insert and update */
	OneToOneOptions<C, O> mandatory();
	
	/**
	 * Defines the bidirectional relation.
	 * No need to additionally call {@link #mappedBy(SerializableAccessor)}.
	 *
	 * If the relation is already defined through {@link #mappedBy(SerializableAccessor)} then there's no
	 * guaranty about which one will be taken first. Algorithm is defined in {@link OneToOneRelationConfigurer}.
	 *
	 * @param reverseLink opposite owner of the relation (setter)
	 * @return the global mapping configurer
	 */
	OneToOneOptions<C, O> mappedBy(SerializableMutator<? super O, C> reverseLink);
	
	/**
	 * Defines the bidirectional relation.
	 * No need to additionally call {@link #mappedBy(SerializableMutator)}.
	 *
	 * If the relation is already defined through {@link #mappedBy(SerializableMutator)} then there's no
	 * guaranty about which one will be taken first. Algorithm is defined in {@link OneToOneRelationConfigurer}.
	 *
	 * @param reverseLink opposite owner of the relation (getter)
	 * @return the global mapping configurer
	 */
	OneToOneOptions<C, O> mappedBy(SerializableAccessor<? super O, C> reverseLink);
	
	/**
	 * Defines reverse column name that stores the relation.
	 * Note that defining it this way will not allow relation to be fixed in memory (after select in database), prefer {@link #mappedBy(SerializableMutator)}.
	 * Use this method to define unidirectional relation.
	 *
	 * If the relation is already defined through {@link #mappedBy(SerializableAccessor)} or {@link #mappedBy(SerializableMutator)} then there's no
	 * guaranty about which one will be taken first. Algorithm is defined in {@link OneToOneRelationConfigurer}.
	 *
	 * @param reverseColumnName opposite owner of the relation
	 * @return the global mapping configurer
	 * @see #columnName(String)
	 */
	OneToOneOptions<C, O> reverseJoinColumn(String reverseColumnName);
	
	/**
	 * Asks to load the relation in some separate query (actually may use several queries according to association table presence or polymorphism)
	 *
	 * @return the global mapping configurer
	 */
	OneToOneOptions<C, O> fetchSeparately();
	
	/**
	 * Gives the name of the column referencing the target entity key (when the relation is owned by current entity)
	 * Valuable only for single key cases.
	 *
	 * @param columnName foreign key column name
	 * @return the global mapping configurer
	 * @see #reverseJoinColumn(String)
	 */
	OneToOneOptions<C, O> columnName(String columnName);
	
	/**
	 * Marks the relation column as a unique key.
	 * Relevant when the unique key constraint corresponds to only a single column.
	 */
	OneToOneOptions<C, O> unique();
}
