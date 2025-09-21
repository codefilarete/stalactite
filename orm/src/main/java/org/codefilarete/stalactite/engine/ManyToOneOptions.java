package org.codefilarete.stalactite.engine;

import org.codefilarete.stalactite.engine.configurer.onetomany.OneToManyRelationConfigurer;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.danekja.java.util.function.serializable.SerializableBiConsumer;
import org.danekja.java.util.function.serializable.SerializableFunction;

/**
 * @author Guillaume Mary
 */
public interface ManyToOneOptions<C, I, T extends Table, O> extends CascadeOptions {
	
//	/** Marks the relation as mandatory. Hence joins will be inner ones and a checking for non null value will be done before insert and update */
//	ManyToOneOptions<C, I, T, O> mandatory();
//
//	/**
//	 * Defines the bidirectional relationship.
//	 * No need to additionally call {@link #mappedBy(SerializableFunction)} or {@link #mappedBy(Column)}.
//	 *
//	 * If the relationship is already defined through {@link #mappedBy(Column)} or {@link #mappedBy(SerializableFunction)} then there's no
//	 * guaranty about which one will be taken first. Algorithm is defined in {@link OneToManyRelationConfigurer}.
//	 *
//	 * @param reverseLink opposite owner of the relation (setter)
//	 * @return the global mapping configurer
//	 */
//	ManyToOneOptions<C, I, T, O> mappedBy(SerializableBiConsumer<? super O, C> reverseLink);
//
//	/**
//	 * Defines the bidirectional relationship.
//	 * No need to additionally call {@link #mappedBy(SerializableBiConsumer)} or {@link #mappedBy(Column)}.
//	 *
//	 * If the relationship is already defined through {@link #mappedBy(Column)} or {@link #mappedBy(SerializableBiConsumer)} then there's no
//	 * guaranty about which one will be taken first. Algorithm is defined in {@link OneToManyRelationConfigurer}.
//	 *
//	 * @param reverseLink opposite owner of the relation (getter)
//	 * @return the global mapping configurer
//	 */
//	ManyToOneOptions<C, I, T, O> mappedBy(SerializableFunction<? super O, C> reverseLink);
//
//	/**
//	 * Defines reverse side owner.
//	 * Note that defining it this way will not allow relation to be fixed in memory (after select in database), prefer {@link #mappedBy(SerializableBiConsumer)}.
//	 * Use this method to define unidirectional relationship.
//	 *
//	 * If the relationship is already defined through {@link #mappedBy(SerializableFunction)} or {@link #mappedBy(SerializableBiConsumer)} then there's no
//	 * guaranty about which one will be taken first. Algorithm is defined in {@link OneToManyRelationConfigurer}.
//	 *
//	 * @param reverseLink opposite owner of the relation
//	 * @return the global mapping configurer
//	 */
//	ManyToOneOptions<C, I, T, O> mappedBy(Column<T, I> reverseLink);
//
//	/**
//	 * Asks to load relation in some separate query (actually may use several queries according to association table presence or polymorphism)
//	 *
//	 * @return the global mapping configurer
//	 */
//	ManyToOneOptions<C, I, T, O> fetchSeparately();
}
