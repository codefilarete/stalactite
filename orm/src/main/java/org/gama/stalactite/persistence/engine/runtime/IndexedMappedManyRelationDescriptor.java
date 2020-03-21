package org.gama.stalactite.persistence.engine.runtime;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Container to store information of a one-to-many indexed mapped relation (by a column on the reverse side)
 * 
 * @author Guillaume Mary
 */
public class IndexedMappedManyRelationDescriptor<I, O, C extends Collection<O>> extends ManyRelationDescriptor<I, O, C> {
	
	/** Getter for getting source entity from reverse side (target entity) */
	private final Function<O, I> reverseGetter;
	
	/**
	 * Getter signature for exception message purpose, can't be deduce from {@code reverseGetter}
	 * because it can be either a {@link org.danekja.java.util.function.serializable.SerializableFunction},
	 * or {@link Function} took from an {@link org.gama.reflection.IAccessor}
	 */
	private final String reverseGetterSignature;
	
	/**
	 * 
	 * @param collectionGetter collection accessor
	 * @param collectionSetter collection setter
	 * @param collectionFactory collection factory
	 * @param reverseSetter setter on the owning side for source bean, optional
	 * @param reverseGetter getter on the owning side for source bean, required to find out entity index into the collection
	 * @param reverseGetterSignature signature of the reverse getter, for better message on exception. Can't be deduce from {@code reverseGetter}
	 * 		because it can be either a {@link org.danekja.java.util.function.serializable.SerializableFunction},
	 * 		or {@link Function} took from an {@link org.gama.reflection.IAccessor}
	 */
	public IndexedMappedManyRelationDescriptor(Function<I, C> collectionGetter,
											   BiConsumer<I, C> collectionSetter,
											   Supplier<C> collectionFactory,
											   @Nullable BiConsumer<O, I> reverseSetter,
											   Function<O, I> reverseGetter,
											   String reverseGetterSignature) {
		super(collectionGetter, collectionSetter, collectionFactory, reverseSetter);
		this.reverseGetter = reverseGetter;
		this.reverseGetterSignature = reverseGetterSignature;
	}
	
	/**
	 * @return getter for getting source entity from reverse side (target entity), required to find out entity index into the collection
	 */
	public Function<O, I> getReverseGetter() {
		return reverseGetter;
	}
	
	/**
	 * Gives getter signature. For exception message purpose, because it can't be deduce from {@code reverseGetter}
	 * because it can be either a {@link org.danekja.java.util.function.serializable.SerializableFunction}
	 * or {@link Function} took from an {@link org.gama.reflection.IAccessor}
	 */
	public String getReverseGetterSignature() {
		return reverseGetterSignature;
	}
}
