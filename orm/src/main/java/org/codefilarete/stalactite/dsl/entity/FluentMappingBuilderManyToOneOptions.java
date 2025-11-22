package org.codefilarete.stalactite.dsl.entity;

import java.util.Collection;
import java.util.function.Supplier;

import org.codefilarete.stalactite.dsl.relation.ManyToOneOptions;
import org.danekja.java.util.function.serializable.SerializableBiConsumer;
import org.danekja.java.util.function.serializable.SerializableFunction;

public interface FluentMappingBuilderManyToOneOptions<C, I, O, S extends Collection<C>> extends FluentEntityMappingBuilder<C, I>,
		ManyToOneOptions<C, I, O, S> {
	
	@Override
	FluentMappingBuilderManyToOneOptions<C, I, O, S> mandatory();
	
	/**
	 * Defines combiner of current entity with target entity. This is a more fine-grained way to define how to combine current entity with target
	 * entity than {@link #reverseCollection(SerializableFunction)} : sometimes a method already exists in entities to fill the relation instead of
	 * calling getter + Collection.add. This method is here to benefit from it.
	 * This method has no consequence on database mapping since it only interacts in memory.
	 *
	 * @param reverseLink opposite owner of the relation
	 * @return the global mapping configurer
	 */
	@Override
	FluentMappingBuilderManyToOneOptions<C, I, O, S> reverselySetBy(SerializableBiConsumer<O, C> reverseLink);
	
	/**
	 * Defines reverse collection accessor.
	 * Used to fill an (in-memory) bi-directionality.
	 * This method has no consequence on database mapping since it only interacts in memory.
	 *
	 * @param collectionAccessor opposite owner of the relation
	 * @return the global mapping configurer
	 */
	@Override
	FluentMappingBuilderManyToOneOptions<C, I, O, S> reverseCollection(SerializableFunction<O, S> collectionAccessor);
	
	/**
	 * Defines reverse collection mutator.
	 * Used to fill an (in-memory) bi-directionality.
	 * This method has no consequence on database mapping since it only interacts in memory.
	 *
	 * @param collectionMutator opposite setter of the relation
	 * @return the global mapping configurer
	 */
	@Override
	FluentMappingBuilderManyToOneOptions<C, I, O, S> reverseCollection(SerializableBiConsumer<O, S> collectionMutator);
	
	/**
	 * Defines the factory of reverse collection. If not defined and collection is found null, the collection is set with a default value.
	 * This method has no consequence on database mapping since it only interacts in memory.
	 *
	 * @param collectionFactory opposite collection factory
	 * @return the global mapping configurer
	 */
	@Override
	FluentMappingBuilderManyToOneOptions<C, I, O, S> reverselyInitializeWith(Supplier<S> collectionFactory);
	
	@Override
	FluentMappingBuilderManyToOneOptions<C, I, O, S> cascading(RelationMode relationMode);
	
	@Override
	FluentMappingBuilderManyToOneOptions<C, I, O, S> fetchSeparately();
	
	@Override
	FluentMappingBuilderManyToOneOptions<C, I, O, S> columnName(String columnName);
}
