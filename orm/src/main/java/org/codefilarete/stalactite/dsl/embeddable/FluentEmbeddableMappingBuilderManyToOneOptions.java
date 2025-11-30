package org.codefilarete.stalactite.dsl.embeddable;

import java.util.Collection;
import java.util.function.Supplier;

import org.codefilarete.stalactite.dsl.relation.ManyToOneOptions;
import org.danekja.java.util.function.serializable.SerializableBiConsumer;
import org.danekja.java.util.function.serializable.SerializableFunction;

/**
 * Mashup of {@link ManyToOneOptions} and {@link FluentEmbeddableMappingBuilder} to make the many-to-one options available in a fluent way while
 * configuring an embeddable bean.
 *
 * @param <C> entity type
 * @param <O> collection elements type
 * @param <S> collection type
 * @author Guillaume Mary
 */
public interface FluentEmbeddableMappingBuilderManyToOneOptions<C, O, S extends Collection<C>> extends FluentEmbeddableMappingBuilder<C>,
		ManyToOneOptions<C, O, S> {
	
	/**
	 * {@inheritDoc}
	 * Declaration overridden to adapt return type to this class.
	 *
	 * @param reverseLink opposite owner of the relation (setter)
	 * @return the global mapping configurer
	 */
	FluentEmbeddableMappingBuilderManyToOneOptions<C, O, S> mappedBy(SerializableBiConsumer<O, ? super C> reverseLink);
	
	/**
	 * {@inheritDoc}
	 * Declaration overridden to adapt return type to this class.
	 *
	 * @param reverseLink opposite owner of the relation (getter)
	 * @return the global mapping configurer
	 */
	FluentEmbeddableMappingBuilderManyToOneOptions<C, O, S> mappedBy(SerializableFunction<O, ? super C> reverseLink);
	
	/**
	 * {@inheritDoc}
	 * Declaration overridden to adapt return type to this class.
	 *
	 * @param reverseColumnName opposite owner of the relation
	 * @return the global mapping configurer
	 */
	FluentEmbeddableMappingBuilderManyToOneOptions<C, O, S> mappedBy(String reverseColumnName);
	
	/**
	 * {@inheritDoc}
	 * Declaration overridden to adapt return type to this class.
	 *
	 * @param reverseLink opposite owner of the relation
	 * @return the global mapping configurer
	 */
	FluentEmbeddableMappingBuilderManyToOneOptions<C, O, S> reverselySetBy(SerializableBiConsumer<O, C> reverseLink);
	
	/**
	 * {@inheritDoc}
	 * Declaration overridden to adapt return type to this class.
	 *
	 * @param relationMode any {@link RelationMode}
	 * @return the global mapping configurer
	 */
	FluentEmbeddableMappingBuilderManyToOneOptions<C, O, S> cascading(RelationMode relationMode);
	
	/**
	 * {@inheritDoc}
	 * Declaration overridden to adapt return type to this class.
	 *
	 * @param collectionFactory a collection factory
	 * @return the global mapping configurer
	 */
	FluentEmbeddableMappingBuilderManyToOneOptions<C, O, S> initializeWith(Supplier<S> collectionFactory);
	
	/**
	 * {@inheritDoc}
	 * Declaration overridden to adapt return type to this class.
	 *
	 * @return the global mapping configurer
	 */
	FluentEmbeddableMappingBuilderManyToOneOptions<C, O, S> fetchSeparately();
	
	/**
	 * {@inheritDoc}
	 * Declaration overridden to adapt return type to this class.
	 *
	 * @param columnName the column name to be used for order persistence
	 * @return the global mapping configurer
	 */
	FluentEmbeddableMappingBuilderManyToOneOptions<C, O, S> indexedBy(String columnName);
	
	/**
	 * {@inheritDoc}
	 * Declaration overridden to adapt return type to this class.
	 *
	 * @return the global mapping configurer
	 */
	FluentEmbeddableMappingBuilderManyToOneOptions<C, O, S> indexed();
}
