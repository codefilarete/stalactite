package org.codefilarete.stalactite.dsl.embeddable;

import org.codefilarete.stalactite.dsl.relation.OneToOneOptions;
import org.danekja.java.util.function.serializable.SerializableBiConsumer;
import org.danekja.java.util.function.serializable.SerializableFunction;

public interface FluentEmbeddableMappingBuilderOneToOneOptions<C, O> extends FluentEmbeddableMappingBuilder<C>,
		OneToOneOptions<C, O> {
	
	/**
	 * {@inheritDoc}
	 * Declaration overridden to adapt return type to this class.
	 *
	 * @return the global mapping configurer
	 */
	@Override
	FluentEmbeddableMappingBuilderOneToOneOptions<C, O> mandatory();
	
	/**
	 * {@inheritDoc}
	 * Declaration overridden to adapt return type to this class.
	 *
	 * @param reverseLink opposite owner of the relation (setter)
	 * @return the global mapping configurer
	 */
	FluentEmbeddableMappingBuilderOneToOneOptions<C, O> mappedBy(SerializableBiConsumer<? super O, C> reverseLink);
	
	/**
	 * {@inheritDoc}
	 * Declaration overridden to adapt return type to this class.
	 *
	 * @param reverseLink opposite owner of the relation (getter)
	 * @return the global mapping configurer
	 */
	@Override
	FluentEmbeddableMappingBuilderOneToOneOptions<C, O> mappedBy(SerializableFunction<? super O, C> reverseLink);
	
	/**
	 * {@inheritDoc}
	 * Declaration overridden to adapt return type to this class.
	 *
	 * @param reverseColumnName opposite owner of the relation
	 * @return the global mapping configurer
	 */
	@Override
	FluentEmbeddableMappingBuilderOneToOneOptions<C, O> mappedBy(String reverseColumnName);
	
	/**
	 * {@inheritDoc}
	 * Declaration overridden to adapt return type to this class.
	 *
	 * @param relationMode any {@link RelationMode}
	 * @return the global mapping configurer
	 */
	@Override
	FluentEmbeddableMappingBuilderOneToOneOptions<C, O> cascading(RelationMode relationMode);
	
	/**
	 * {@inheritDoc}
	 * Declaration overridden to adapt return type to this class.
	 *
	 * @return the global mapping configurer
	 */
	@Override
	FluentEmbeddableMappingBuilderOneToOneOptions<C, O> fetchSeparately();
	
	/**
	 * {@inheritDoc}
	 * Declaration overridden to adapt return type to this class.
	 *
	 * @return the global mapping configurer
	 */
	@Override
	FluentEmbeddableMappingBuilderOneToOneOptions<C, O> columnName(String columnName);
	
	/**
	 * {@inheritDoc}
	 * Overridden for return type accuracy
	 *
	 * @return the global mapping configurer
	 */
	@Override
	FluentEmbeddableMappingBuilderOneToOneOptions<C, O> unique();
}
