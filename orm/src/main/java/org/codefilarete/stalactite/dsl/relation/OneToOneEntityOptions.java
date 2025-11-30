package org.codefilarete.stalactite.dsl.relation;

import org.codefilarete.stalactite.engine.configurer.onetoone.OneToOneRelationConfigurer;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.danekja.java.util.function.serializable.SerializableBiConsumer;
import org.danekja.java.util.function.serializable.SerializableFunction;

/**
 * @param <C> entity type
 * @param <I> entity identifier type
 * @param <O> target entity type
 * @author Guillaume Mary
 */
public interface OneToOneEntityOptions<C, I, O> extends OneToOneOptions<C, O> {
	
	/**
	 * {@inheritDoc}
	 * Overridden for return type accuracy
	 */
	OneToOneEntityOptions<C, I, O> mandatory();
	
	/**
	 * Defines the bidirectional relation.
	 * No need to additionally call {@link #mappedBy(SerializableFunction)} or {@link #mappedBy(Column)}.
	 *
	 * If the relation is already defined through {@link #mappedBy(Column)} or {@link #mappedBy(SerializableFunction)} then there's no
	 * guaranty about which one will be taken first. Algorithm is defined in {@link OneToOneRelationConfigurer}.
	 *
	 * Overridden for return type accuracy
	 *
	 * @param reverseLink opposite owner of the relation (setter)
	 * @return the global mapping configurer
	 */
	OneToOneEntityOptions<C, I, O> mappedBy(SerializableBiConsumer<? super O, C> reverseLink);
	
	/**
	 * Defines the bidirectional relation.
	 * No need to additionally call {@link #mappedBy(SerializableBiConsumer)} or {@link #mappedBy(Column)}.
	 *
	 * If the relation is already defined through {@link #mappedBy(Column)} or {@link #mappedBy(SerializableBiConsumer)} then there's no
	 * guaranty about which one will be taken first. Algorithm is defined in {@link OneToOneRelationConfigurer}.
	 *
	 * @param reverseLink opposite owner of the relation (getter)
	 * @return the global mapping configurer
	 */
	OneToOneEntityOptions<C, I, O> mappedBy(SerializableFunction<? super O, C> reverseLink);
	
	/**
	 * {@inheritDoc}
	 * Overridden for return type accuracy
	 *
	 * @param reverseColumnName opposite owner of the relation
	 * @return the global mapping configurer
	 */
	OneToOneEntityOptions<C, I, O> mappedBy(String reverseColumnName);
	
	/**
	 * {@inheritDoc}
	 * Overridden for return type accuracy
	 *
	 * @return the global mapping configurer
	 */
	OneToOneEntityOptions<C, I, O> fetchSeparately();
	
	/**
	 * {@inheritDoc}
	 * Overridden for return type accuracy
	 *
	 * @return the global mapping configurer
	 */
	OneToOneOptions<C, O> columnName(String columnName);
	
	/**
	 * {@inheritDoc}
	 * Overridden for return type accuracy
	 *
	 * @return the global mapping configurer
	 */
	OneToOneEntityOptions<C, I, O> unique();
	
	/**
	 * Defines reverse side owning column.
	 * Note that defining it this way will not allow relation to be fixed in memory (after select in database), prefer {@link #mappedBy(SerializableBiConsumer)}.
	 * Use this method to define unidirectional relation.
	 *
	 * If the relation is already defined through {@link #mappedBy(SerializableFunction)} or {@link #mappedBy(SerializableBiConsumer)} then there's no
	 * guaranty about which one will be taken first. Algorithm is defined in {@link OneToOneRelationConfigurer}.
	 *
	 * @param reverseLink opposite owner of the relation
	 * @return the global mapping configurer
	 */
	OneToOneEntityOptions<C, I, O> mappedBy(Column<?, I> reverseLink);
	
}
