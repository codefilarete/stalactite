package org.gama.stalactite.persistence.engine.runtime;

import java.util.Collection;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Container to store information of a one-to-many relation
 * 
 * @author Guillaume Mary
 */
public class ManyRelationDescriptor<I, O, C extends Collection<O>> {
	
	private final Function<I, C> collectionGetter;
	
	private final BiConsumer<I, C> collectionSetter;
	
	private final Supplier<C> collectionFactory;
	
	/**
	 *
	 * @param collectionGetter collection accessor
	 * @param collectionSetter collection setter
	 * @param collectionFactory collection factory
	 */
	public ManyRelationDescriptor(Function<I, C> collectionGetter, BiConsumer<I, C> collectionSetter, Supplier<C> collectionFactory) {
		this.collectionGetter = collectionGetter;
		this.collectionSetter = collectionSetter;
		this.collectionFactory = collectionFactory;
	}
	
	public Function<I, C> getCollectionGetter() {
		return collectionGetter;
	}
	
	public BiConsumer<I, C> getCollectionSetter() {
		return collectionSetter;
	}
	
	public Supplier<C> getCollectionFactory() {
		return collectionFactory;
	}
}
