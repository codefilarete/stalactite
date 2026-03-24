package org.codefilarete.stalactite.dsl.embeddable;

import java.util.Collection;
import java.util.function.Supplier;

import org.codefilarete.reflection.SerializablePropertyAccessor;
import org.codefilarete.reflection.SerializablePropertyMutator;
import org.codefilarete.stalactite.dsl.relation.ManyToManyOptions;

/**
 * Mashup of {@link ManyToManyOptions} and {@link FluentEmbeddableMappingBuilder} to make the many-to-many options available in a fluent way while
 * configuring an embeddable bean.
 *
 * @param <C> entity type
 * @param <O> collection elements type
 * @param <S1> collection type
 * @param <S2> reverse collection type
 * @author Guillaume Mary
 */
public interface FluentEmbeddableMappingBuilderManyToManyOptions<C, O, S1 extends Collection<O>, S2 extends Collection<C>> extends FluentEmbeddableMappingBuilder<C>,
		ManyToManyOptions<C, O, S1, S2> {
	
	/**
	 * {@inheritDoc}
	 * Declaration overridden to adapt return type to this class.
	 *
	 * @param collectionFactory a collection factory
	 * @return the global mapping configurer
	 */
	FluentEmbeddableMappingBuilderManyToManyOptions<C, O, S1, S2> initializeWith(Supplier<S1> collectionFactory);
	
	/**
	 * {@inheritDoc}
	 * Declaration overridden to adapt return type to this class.
	 *
	 * @param reverseLink opposite owner of the relation
	 * @return the global mapping configurer
	 */
	FluentEmbeddableMappingBuilderManyToManyOptions<C, O, S1, S2> reverselySetBy(SerializablePropertyMutator<O, C> reverseLink);
	
	/**
	 * {@inheritDoc}
	 * Declaration overridden to adapt return type to this class.
	 *
	 * @param collectionAccessor opposite owner of the relation
	 * @return the global mapping configurer
	 */
	FluentEmbeddableMappingBuilderManyToManyOptions<C, O, S1, S2> reverseCollection(SerializablePropertyAccessor<O, S2> collectionAccessor);
	
	/**
	 * {@inheritDoc}
	 * Declaration overridden to adapt return type to this class.
	 *
	 * @param collectionMutator opposite setter of the relation
	 * @return the global mapping configurer
	 */
	FluentEmbeddableMappingBuilderManyToManyOptions<C, O, S1, S2> reverseCollection(SerializablePropertyMutator<O, S2> collectionMutator);
	
	/**
	 * {@inheritDoc}
	 * Declaration overridden to adapt return type to this class.
	 *
	 * @param collectionFactory opposite collection factory
	 * @return the global mapping configurer
	 */
	FluentEmbeddableMappingBuilderManyToManyOptions<C, O, S1, S2> reverselyInitializeWith(Supplier<S2> collectionFactory);
	
	/**
	 * {@inheritDoc}
	 * Declaration overridden to adapt return type to this class.
	 *
	 * @return the global mapping configurer
	 */
	FluentEmbeddableMappingBuilderManyToManyOptions<C, O, S1, S2> fetchSeparately();
	
	/**
	 * {@inheritDoc}
	 * Declaration overridden to adapt return type to this class.
	 *
	 * @return the global mapping configurer
	 */
	FluentEmbeddableMappingBuilderManyToManyOptions<C, O, S1, S2> indexedBy(String columnName);
	
	/**
	 * {@inheritDoc}
	 * Declaration overridden to adapt return type to this class.
	 *
	 * @return the global mapping configurer
	 */
	FluentEmbeddableMappingBuilderManyToManyOptions<C, O, S1, S2> indexed();
}
