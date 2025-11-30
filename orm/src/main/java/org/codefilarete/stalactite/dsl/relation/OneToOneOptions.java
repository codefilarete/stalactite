package org.codefilarete.stalactite.dsl.relation;

import org.codefilarete.stalactite.dsl.property.CascadeOptions;
import org.codefilarete.stalactite.engine.configurer.onetoone.OneToOneRelationConfigurer;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.danekja.java.util.function.serializable.SerializableBiConsumer;
import org.danekja.java.util.function.serializable.SerializableFunction;

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
	 * No need to additionally call {@link #mappedBy(SerializableFunction)}.
	 *
	 * If the relation is already defined through {@link #mappedBy(SerializableFunction)} then there's no
	 * guaranty about which one will be taken first. Algorithm is defined in {@link OneToOneRelationConfigurer}.
	 *
	 * @param reverseLink opposite owner of the relation (setter)
	 * @return the global mapping configurer
	 */
	OneToOneOptions<C, O> mappedBy(SerializableBiConsumer<? super O, C> reverseLink);
	
	/**
	 * Defines the bidirectional relation.
	 * No need to additionally call {@link #mappedBy(SerializableBiConsumer)}.
	 *
	 * If the relation is already defined through {@link #mappedBy(SerializableBiConsumer)} then there's no
	 * guaranty about which one will be taken first. Algorithm is defined in {@link OneToOneRelationConfigurer}.
	 *
	 * @param reverseLink opposite owner of the relation (getter)
	 * @return the global mapping configurer
	 */
	OneToOneOptions<C, O> mappedBy(SerializableFunction<? super O, C> reverseLink);
	
	/**
	 * Defines reverse side owning column name.
	 * Note that defining it this way will not allow relation to be fixed in memory (after select in database), prefer {@link #mappedBy(SerializableBiConsumer)}.
	 * Use this method to define unidirectional relation.
	 *
	 * If the relation is already defined through {@link #mappedBy(SerializableFunction)} or {@link #mappedBy(SerializableBiConsumer)} then there's no
	 * guaranty about which one will be taken first. Algorithm is defined in {@link OneToOneRelationConfigurer}.
	 *
	 * @param reverseColumnName opposite owner of the relation
	 * @return the global mapping configurer
	 */
	OneToOneOptions<C, O> mappedBy(String reverseColumnName);
	
	/**
	 * Asks to load the relation in some separate query (actually may use several queries according to association table presence or polymorphism)
	 *
	 * @return the global mapping configurer
	 */
	OneToOneOptions<C, O> fetchSeparately();
	
	/**
	 * Give the column name of the foreign key column referencing the target entity key.
	 * Valuable only for single key cases.
	 *
	 * @param columnName foreign key column name
	 * @return the global mapping configurer
	 */
	OneToOneOptions<C, O> columnName(String columnName);
	
	/**
	 * Marks the relation column as a unique key.
	 * Relevant when the unique key constraint corresponds to only a single column.
	 */
	OneToOneOptions<C, O> unique();
}
