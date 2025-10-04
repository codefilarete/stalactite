package org.codefilarete.stalactite.dsl.relation;

import java.util.Collection;
import java.util.function.Supplier;

import org.codefilarete.stalactite.dsl.property.CascadeOptions;
import org.danekja.java.util.function.serializable.SerializableBiConsumer;
import org.danekja.java.util.function.serializable.SerializableFunction;

/**
 * @author Guillaume Mary
 */
public interface ManyToManyOptions<C, I, O, S1 extends Collection<O>, S2 extends Collection<C>> extends CascadeOptions {
	
	/**
	 * Defines the collection factory to be used at load time to initialize property if it is null.
	 * Useful for cases where property is lazily initialized in bean.
	 * 
	 * @param collectionFactory a collection factory
	 * @return the global mapping configurer
	 */
	ManyToManyOptions<C, I, O, S1, S2> initializeWith(Supplier<S1> collectionFactory);
	
	/**
	 * Defines combiner of current entity with target entity. This is a more fine-grained way to define how to combine current entity with target
	 * entity than {@link #reverseCollection(SerializableFunction)} : sometimes a method already exists in entities to fill the relation instead of
	 * calling getter + Collection.add. This method is here to benefit from it.
	 * This method has no consequence on database mapping since it only interacts in memory.
	 *
	 * @param reverseLink opposite owner of the relation
	 * @return the global mapping configurer
	 */
	ManyToManyOptions<C, I, O, S1, S2> reverselySetBy(SerializableBiConsumer<O, C> reverseLink);
	
	/**
	 * Defines reverse collection accessor.
	 * Used to fill an (in-memory) bi-directionality.
	 * This method has no consequence on database mapping since it only interacts in memory.
	 *
	 * @param collectionAccessor opposite owner of the relation
	 * @return the global mapping configurer
	 */
	ManyToManyOptions<C, I, O, S1, S2> reverseCollection(SerializableFunction<O, S2> collectionAccessor);
	
	/**
	 * Defines reverse collection mutator.
	 * Used to fill an (in-memory) bi-directionality.
	 * This method has no consequence on database mapping since it only interacts in memory.
	 *
	 * @param collectionMutator opposite setter of the relation
	 * @return the global mapping configurer
	 */
	ManyToManyOptions<C, I, O, S1, S2> reverseCollection(SerializableBiConsumer<O, S2> collectionMutator);
	
	/**
	 * Defines the factory of reverse collection. If not defined and collection is found null, the collection is set with a default value.
	 * This method has no consequence on database mapping since it only interacts in memory.
	 *
	 * @param collectionFactory opposite collection factory
	 * @return the global mapping configurer
	 */
	ManyToManyOptions<C, I, O, S1, S2> reverselyInitializeWith(Supplier<S2> collectionFactory);
	
	/**
	 * Asks to load relation in some separate query (actually may use several queries according to association table presence or polymorphism)
	 * 
	 * @return the global mapping configurer
	 */
	ManyToManyOptions<C, I, O, S1, S2> fetchSeparately();
	
	/**
	 * Activates entity order persistence and indicates column name to be used for it.
	 * Collection that stores data is expected to support ordering by index (as List or LinkedHashSet)
	 *
	 * @return the global mapping configurer
	 */
	ManyToManyOptions<C, I, O, S1, S2> indexedBy(String columnName);
	
	/**
	 * Activates entity order persistence.
	 * Collection that stores data is expected to support ordering by index (as List or LinkedHashSet)
	 *
	 * @return the global mapping configurer
	 */
	ManyToManyOptions<C, I, O, S1, S2> indexed();
	
}
