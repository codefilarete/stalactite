package org.codefilarete.stalactite.engine;

import java.util.Collection;
import java.util.function.Supplier;

import org.danekja.java.util.function.serializable.SerializableBiConsumer;
import org.danekja.java.util.function.serializable.SerializableFunction;

/**
 * @author Guillaume Mary
 */
public interface ManyToOneOptions<C, I, O, S extends Collection<C>> extends CascadeOptions {
	
	/** Marks the relation as mandatory. Hence joins will be inner ones and a checking for non null value will be done before insert and update */
	ManyToOneOptions<C, I, O, S> mandatory();

	/**
	 * Defines combiner of current entity with target entity. This is a more fine-grained way to define how to combine current entity with target
	 * entity than {@link #reverseCollection(SerializableFunction)} : sometimes a method already exists in entities to fill the relation instead of
	 * calling getter + Collection.add. This method is here to benefit from it.
	 * This method has no consequence on database mapping since it only interacts in memory.
	 *
	 * @param reverseLink opposite owner of the relation
	 * @return the global mapping configurer
	 */
	ManyToOneOptions<C, I, O, S> reverselySetBy(SerializableBiConsumer<O, C> reverseLink);
	
	/**
	 * Defines reverse collection accessor.
	 * Used to fill an (in-memory) bi-directionality.
	 * This method has no consequence on database mapping since it only interacts in memory.
	 *
	 * @param collectionAccessor opposite owner of the relation
	 * @return the global mapping configurer
	 */
	ManyToOneOptions<C, I, O, S> reverseCollection(SerializableFunction<O, S> collectionAccessor);
	
	/**
	 * Defines reverse collection mutator.
	 * Used to fill an (in-memory) bi-directionality.
	 * This method has no consequence on database mapping since it only interacts in memory.
	 *
	 * @param collectionMutator opposite setter of the relation
	 * @return the global mapping configurer
	 */
	ManyToOneOptions<C, I, O, S> reverseCollection(SerializableBiConsumer<O, S> collectionMutator);
	
	/**
	 * Defines the factory of reverse collection. If not defined and collection is found null, the collection is set with a default value.
	 * This method has no consequence on database mapping since it only interacts in memory.
	 *
	 * @param collectionFactory opposite collection factory
	 * @return the global mapping configurer
	 */
	ManyToOneOptions<C, I, O, S> reverselyInitializeWith(Supplier<S> collectionFactory);
	
	/**
	 * Asks to load relation in some separate query (actually may use several queries according to association table presence or polymorphism)
	 *
	 * @return the global mapping configurer
	 */
	ManyToOneOptions<C, I, O, S> fetchSeparately();
}
