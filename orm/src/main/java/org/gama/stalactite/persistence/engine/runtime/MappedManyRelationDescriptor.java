package org.gama.stalactite.persistence.engine.runtime;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.function.BiConsumer;
import java.util.function.Function;

/**
 * Container to store information of a one-to-many mapped relation (by a column on the reverse side)
 * 
 * @author Guillaume Mary
 */
public class MappedManyRelationDescriptor<I, O, C extends Collection<O>> extends ManyRelationDescriptor<I, O, C> {
	
	/** Setter for applying source entity to reverse side (target entity). Available only when association is mapped without intermediary table */
	private final BiConsumer<O, I> reverseSetter;
	
	/**
	 * 
	 * @param collectionGetter collection accessor
	 * @param collectionSetter collection setter
	 * @param collectionClass collection type
	 * @param reverseSetter setter on the owning side for source bean, optional
	 */
	public MappedManyRelationDescriptor(Function<I, C> collectionGetter,
										BiConsumer<I, C> collectionSetter,
										Class<C> collectionClass,
										@Nullable BiConsumer<O, I> reverseSetter) {
		super(collectionGetter, collectionSetter, collectionClass);
		this.reverseSetter = reverseSetter;
	}
	
	/**
	 * Gives the setter for source bean on the owning side. 
	 * 
	 * @return null if no setter given at construction time
	 */
	@Nullable
	public BiConsumer<O, I> getReverseSetter() {
		return reverseSetter;
	}
}
