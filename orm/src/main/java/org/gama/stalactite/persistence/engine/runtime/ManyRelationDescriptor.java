package org.gama.stalactite.persistence.engine.runtime;

import java.util.Collection;
import java.util.function.BiConsumer;
import java.util.function.Function;

/**
 * Container to store information of a one-to-many relation
 * 
 * @author Guillaume Mary
 */
public class ManyRelationDescriptor<I, O, C extends Collection<O>> {
	
	private final Function<I, C> collectionGetter;
	
	private final BiConsumer<I, C> collectionSetter;
	
	private final Class<C> collectionClass;
	
	/**
	 *
	 * @param collectionGetter collection accessor
	 * @param collectionSetter collection setter
	 * @param collectionClass collection type
	 */
	public ManyRelationDescriptor(Function<I, C> collectionGetter, BiConsumer<I, C> collectionSetter, Class<C> collectionClass) {
		this.collectionGetter = collectionGetter;
		this.collectionSetter = collectionSetter;
		this.collectionClass = collectionClass;
	}
	
	public Function<I, C> getCollectionGetter() {
		return collectionGetter;
	}
	
	public BiConsumer<I, C> getCollectionSetter() {
		return collectionSetter;
	}
	
	public Class<C> getCollectionClass() {
		return collectionClass;
	}
}
