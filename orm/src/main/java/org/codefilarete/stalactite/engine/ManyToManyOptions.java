package org.codefilarete.stalactite.engine;

import java.util.Collection;
import java.util.function.Supplier;

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
	
	ManyToManyOptions<C, I, O, S1, S2> reverseInitializeWith(Supplier<S2> collectionFactory);
	
	/**
	 * Asks to load relation in some separate query (actually may use several queries according to association table presence or polymorphism)
	 * 
	 * @return the global mapping configurer
	 */
	ManyToManyOptions<C, I, O, S1, S2> fetchSeparately();
	
}
