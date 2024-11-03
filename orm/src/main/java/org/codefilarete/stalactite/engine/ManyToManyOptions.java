package org.codefilarete.stalactite.engine;

import java.util.Collection;
import java.util.function.Supplier;

import org.danekja.java.util.function.serializable.SerializableBiConsumer;

/**
 * @author Guillaume Mary
 */
public interface ManyToManyOptions<C, I, O, S1 extends Collection<O>, S2 extends Collection<C>> extends CascadeOptions {
	
	/**
	 * Defines setter of current entity on target entity.
	 * This method has no consequence on database mapping since it only interacts in memory.
	 *
	 * @param reverseLink opposite owner of the relation
	 * @return the global mapping configurer
	 */
	ManyToManyOptions<C, I, O, S1, S2> reverselySetBy(SerializableBiConsumer<O, C> reverseLink);
	
	/**
	 * Defines the collection factory to be used at load time to initialize property if it is null.
	 * Useful for cases where property is lazily initialized in bean.
	 * 
	 * @param collectionFactory a collection factory
	 * @return the global mapping configurer
	 */
	ManyToManyOptions<C, I, O, S1, S2> initializeWith(Supplier<S1> collectionFactory);
	
	ManyToManyOptions<C, I, O, S1, S2> reverseInitializeWith(Supplier<S2> collectionFactory);
	
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
