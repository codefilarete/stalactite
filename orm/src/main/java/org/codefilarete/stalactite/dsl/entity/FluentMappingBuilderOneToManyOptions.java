package org.codefilarete.stalactite.dsl.entity;

import java.util.Collection;
import java.util.function.Supplier;

import org.codefilarete.stalactite.dsl.relation.OneToManyEntityOptions;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.danekja.java.util.function.serializable.SerializableBiConsumer;
import org.danekja.java.util.function.serializable.SerializableFunction;

/**
 * Mashup of {@link FluentEntityMappingBuilder} and {@link OneToManyEntityOptions} to let one configure both through a fluent API.
 *
 * @param <C> entity type
 * @param <I> entity identifier type
 * @param <O> type of {@link Collection} element
 * @param <S> refined {@link Collection} type
 *
 * @author Guillaume Mary
 */
public interface FluentMappingBuilderOneToManyOptions<C, I, O, S extends Collection<O>> extends FluentEntityMappingBuilder<C, I>, OneToManyEntityOptions<C, I, O, S> {
	
	/**
	 * {@inheritDoc}
	 * Declaration overridden to adapt return type to this class.
	 *
	 * @param reverseLink opposite owner of the relation (setter)
	 * @return the global mapping configurer
	 */
	@Override
	FluentMappingBuilderOneToManyMappedByOptions<C, I, O, S> mappedBy(SerializableBiConsumer<O, ? super C> reverseLink);
	
	/**
	 * {@inheritDoc}
	 * Declaration overridden to adapt return type to this class.
	 *
	 * @param reverseLink opposite owner of the relation (setter)
	 * @return the global mapping configurer
	 */
	@Override
	FluentMappingBuilderOneToManyMappedByOptions<C, I, O, S> mappedBy(SerializableFunction<O, ? super C> reverseLink);
	
	/**
	 * {@inheritDoc}
	 * Declaration overridden to adapt return type to this class.
	 *
	 * @param reverseLink opposite owner of the relation (setter)
	 * @return the global mapping configurer
	 */
	@Override
	FluentMappingBuilderOneToManyMappedByOptions<C, I, O, S> mappedBy(Column<?, I> reverseLink);
	
	/**
	 *
	 * @param reverseColumnName opposite owner of the relation
	 * @return the global mapping configurer
	 */
	@Override
	FluentMappingBuilderOneToManyMappedByOptions<C, I, O, S> reverseJoinColumn(String reverseColumnName);
	
	/**
	 * {@inheritDoc}
	 * Declaration overridden to adapt return type to this class.
	 *
	 * @param reverseLink opposite owner of the relation
	 * @return the global mapping configurer
	 */
	@Override
	FluentMappingBuilderOneToManyOptions<C, I, O, S> reverselySetBy(SerializableBiConsumer<O, C> reverseLink);
	
	/**
	 * {@inheritDoc}
	 * Declaration overridden to adapt return type to this class.
	 *
	 * @param collectionFactory a collection factory
	 * @return the global mapping configurer
	 */
	@Override
	FluentMappingBuilderOneToManyOptions<C, I, O, S> initializeWith(Supplier<S> collectionFactory);
	
	/**
	 * {@inheritDoc}
	 * Declaration overridden to adapt return type to this class.
	 *
	 * @param relationMode any {@link RelationMode}
	 * @return the global mapping configurer
	 */
	@Override
	FluentMappingBuilderOneToManyOptions<C, I, O, S> cascading(RelationMode relationMode);
	
	/**
	 * {@inheritDoc}
	 * Declaration overridden to adapt return type to this class.
	 *
	 * @return the global mapping configurer
	 */
	@Override
	FluentMappingBuilderOneToManyOptions<C, I, O, S> fetchSeparately();
	
	/**
	 * {@inheritDoc}
	 * Declaration overridden to adapt return type to this class.
	 *
	 * @param orderingColumn orderingColumn the column to be used for order persistence
	 * @return the global mapping configurer
	 */
	@Override
	FluentMappingBuilderOneToManyOptions<C, I, O, S> indexedBy(Column<?, Integer> orderingColumn);
	
	/**
	 * {@inheritDoc}
	 * Declaration overridden to adapt return type to this class.
	 *
	 * @param columnName the column name to be used for order persistence
	 * @return the global mapping configurer
	 */
	@Override
	FluentMappingBuilderOneToManyOptions<C, I, O, S> indexedBy(String columnName);
	
	/**
	 * {@inheritDoc}
	 * Declaration overridden to adapt return type to this class.
	 *
	 * @return the global mapping configurer
	 */
	@Override
	FluentMappingBuilderOneToManyOptions<C, I, O, S> indexed();
	
}
