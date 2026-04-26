package org.codefilarete.stalactite.dsl.relation;

import org.codefilarete.reflection.SerializablePropertyAccessor;
import org.codefilarete.reflection.SerializablePropertyMutator;
import org.codefilarete.stalactite.engine.configurer.onetoone.OneToOneRelationConfigurer;
import org.codefilarete.stalactite.sql.ddl.structure.Column;

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
	 * No need to additionally call {@link OneToOneOptions#mappedBy(SerializablePropertyAccessor)} or {@link #reverseJoinColumn(Column)}.
	 *
	 * If the relation is already defined through {@link #reverseJoinColumn(Column)} or {@link OneToOneOptions#mappedBy(SerializablePropertyAccessor)} then there's no
	 * guaranty about which one will be taken first. Algorithm is defined in {@link OneToOneRelationConfigurer}.
	 *
	 * Overridden for return type accuracy
	 *
	 * @param reverseLink opposite owner of the relation (setter)
	 * @return the global mapping configurer
	 */
	OneToOneEntityOptions<C, I, O> mappedBy(SerializablePropertyMutator<? super O, C> reverseLink);
	
	/**
	 * Defines the bidirectional relation.
	 * No need to additionally call {@link OneToOneOptions#mappedBy(SerializablePropertyMutator)} or {@link #reverseJoinColumn(Column)}.
	 *
	 * If the relation is already defined through {@link #reverseJoinColumn(Column)} or {@link OneToOneOptions#mappedBy(SerializablePropertyMutator)} then there's no
	 * guaranty about which one will be taken first. Algorithm is defined in {@link OneToOneRelationConfigurer}.
	 *
	 * @param reverseLink opposite owner of the relation (getter)
	 * @return the global mapping configurer
	 */
	OneToOneEntityOptions<C, I, O> mappedBy(SerializablePropertyAccessor<? super O, C> reverseLink);
	
	/**
	 * {@inheritDoc}
	 * Overridden for return type accuracy
	 *
	 * @param reverseColumnName opposite owner of the relation
	 * @return the global mapping configurer
	 */
	OneToOneEntityOptions<C, I, O> reverseJoinColumn(String reverseColumnName);
	
	/**
	 * Defines reverse side owning column.
	 * Note that defining it this way will not allow relation to be fixed in memory (after select in database), prefer {@link OneToOneOptions#mappedBy(SerializablePropertyMutator)}.
	 * Use this method to define unidirectional relation.
	 *
	 * If the relation is already defined through {@link OneToOneOptions#mappedBy(SerializablePropertyAccessor)} or {@link OneToOneOptions#mappedBy(SerializablePropertyMutator)} then there's no
	 * guaranty about which one will be taken first. Algorithm is defined in {@link OneToOneRelationConfigurer}.
	 *
	 * @param reverseLink opposite owner of the relation
	 * @return the global mapping configurer
	 */
	OneToOneEntityOptions<C, I, O> reverseJoinColumn(Column<?, I> reverseLink);
	
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
	
}
