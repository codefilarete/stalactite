package org.codefilarete.stalactite.dsl.embeddable;

import java.util.Collection;
import java.util.function.Supplier;

import org.codefilarete.stalactite.dsl.relation.OneToManyOptions;
import org.danekja.java.util.function.serializable.SerializableBiConsumer;
import org.danekja.java.util.function.serializable.SerializableFunction;

/**
 *
 * @param <C> entity type
 * @param <O> collection elements type
 * @param <S> collection type
 * @author Guillaume Mary
 */
public interface FluentEmbeddableMappingBuilderOneToManyOptions<C, O, S extends Collection<O>> extends FluentEmbeddableMappingBuilder<C>,
		OneToManyOptions<C, O, S> {
	
	/**
	 * {@inheritDoc}
	 * Declaration overridden to adapt return type to this class.
	 *
	 * @param reverseLink opposite owner of the relation (setter)
	 * @return the global mapping configurer
	 */
	FluentEmbeddableMappingBuilderOneToManyOptions<C, O, S> mappedBy(SerializableBiConsumer<O, ? super C> reverseLink);
	
	/**
	 * {@inheritDoc}
	 * Declaration overridden to adapt return type to this class.
	 *
	 * @param reverseLink opposite owner of the relation (getter)
	 * @return the global mapping configurer
	 */
	FluentEmbeddableMappingBuilderOneToManyOptions<C, O, S> mappedBy(SerializableFunction<O, ? super C> reverseLink);
	
	/**
	 * {@inheritDoc}
	 * Declaration overridden to adapt return type to this class.
	 *
	 * @param reverseColumnName opposite owner of the relation
	 * @return the global mapping configurer
	 */
	FluentEmbeddableMappingBuilderOneToManyOptions<C, O, S> reverseJoinColumn(String reverseColumnName);
	
	/**
	 * {@inheritDoc}
	 * Declaration overridden to adapt return type to this class.
	 *
	 * @param tableName the table name of the association table
	 * @return the global mapping configurer
	 */
	FluentEmbeddableMappingBuilderOneToManyOptions<C, O, S> joinTable(String tableName);
	
	/**
	 * {@inheritDoc}
	 * Declaration overridden to adapt return type to this class.
	 *
	 * @param reverseLink opposite owner of the relation
	 * @return the global mapping configurer
	 */
	FluentEmbeddableMappingBuilderOneToManyOptions<C, O, S> reverselySetBy(SerializableBiConsumer<O, C> reverseLink);
	
	/**
	 * {@inheritDoc}
	 * Declaration overridden to adapt return type to this class.
	 *
	 * @param relationMode any {@link RelationMode}
	 * @return the global mapping configurer
	 */
	FluentEmbeddableMappingBuilderOneToManyOptions<C, O, S> cascading(RelationMode relationMode);
	
	/**
	 * {@inheritDoc}
	 * Declaration overridden to adapt return type to this class.
	 *
	 * @param collectionFactory a collection factory
	 * @return the global mapping configurer
	 */
	FluentEmbeddableMappingBuilderOneToManyOptions<C, O, S> initializeWith(Supplier<S> collectionFactory);
	
	/**
	 * {@inheritDoc}
	 * Declaration overridden to adapt return type to this class.
	 *
	 * @return the global mapping configurer
	 */
	FluentEmbeddableMappingBuilderOneToManyOptions<C, O, S> fetchSeparately();
	
	/**
	 * {@inheritDoc}
	 * Declaration overridden to adapt return type to this class.
	 *
	 * @param columnName the column name to be used for order persistence
	 * @return the global mapping configurer
	 */
	FluentEmbeddableMappingBuilderOneToManyOptions<C, O, S> indexedBy(String columnName);
	
	/**
	 * {@inheritDoc}
	 * Declaration overridden to adapt return type to this class.
	 *
	 * @return the global mapping configurer
	 */
	FluentEmbeddableMappingBuilderOneToManyOptions<C, O, S> indexed();
}
