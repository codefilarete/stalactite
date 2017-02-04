package org.gama.stalactite.persistence.engine;

import java.util.Collection;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;

import org.gama.lang.Reflections;

/**
 * An interface aimed at abstracting the way how relations between 2 beans are filled : implementation should handle one-to-one relationship
 * as well as one-to-many relationship.
 * Since implemenations are quite simple, they are done thru all "of" static methods in this interface.
 * 
 * @author Guillaume Mary
 * @see #of(BiConsumer, Function, Class)
 * @see #of(BiConsumer, Function, Supplier)
 * @see #of(BiConsumer)
 */
@FunctionalInterface
public interface BeanRelationFixer<S, I> {
	
	/**
	 * Main method that fills the relation
	 * 
	 * @param target the owner of the relation
	 * @param input the objet to be writen/added into the relation
	 */
	void apply(S target, I input);
	
	/**
	 * Shortcut to create a {@link BeanRelationFixer} for a one-to-one relation.
	 * 
	 * @param setter the method that fixes the relation
	 * @return a {@link BeanRelationFixer} mapped to {@link BiConsumer#accept(Object, Object)}
	 */
	static <S, I> BeanRelationFixer<S, I> of(BiConsumer<S, I> setter) {
		return of(setter, (a, b) -> { /* no bi-directional relation, nothing to do */ });
	}
	
	/**
	 * Shortcut to create a {@link BeanRelationFixer} for a bidirectionnal one-to-one relation.
	 *
	 * @param setter the method that fixes the relation
	 * @return a {@link BeanRelationFixer} mapped to {@link BiConsumer#accept(Object, Object)}
	 */
	static <S, I> BeanRelationFixer<S, I> of(BiConsumer<S, I> setter, BiConsumer<I, S> reverseSetter) {
		return (s, i) -> {
			setter.accept(s, i);
			// bidirectional assignment
			reverseSetter.accept(i, s);
		};
	}
	
	/**
	 * Shortcut to {@link #of(BiConsumer, Function, Supplier)} with a supplier that will isntanciate the given concrete Collection class
	 * 
	 * @param setter the method that sets the {@link Collection} onto the target bean
	 * @param getter the method that gets the {@link Collection} from the target bean
	 * @param concreteCollectionType the Class that will be instanciated to fill the relation if it is null
	 * @return a {@link BeanRelationFixer} that will add the input to the Collection and create if the getter returns null
	 */
	static <S, I, C extends Collection<I>> BeanRelationFixer<S, I> of(BiConsumer<S, C> setter, Function<S, C> getter,
																	  Class<? extends C> concreteCollectionType) {
		return of(setter, getter, () -> Reflections.newInstance(concreteCollectionType));
	}
	
	/**
	 * Shortcut to create a {@link BeanRelationFixer} for a one-to-many relation where the attribute is a {@link Collection}
	 * 
	 * @param setter the method that sets the {@link Collection} onto the target bean
	 * @param getter the method that gets the {@link Collection} from the target bean
	 * @param collectionFactory a supplier of an instance to fill the relation if it is null
	 * @return a {@link BeanRelationFixer} that will add the input to the Collection and create if the getter returns null
	 */
	static <S, I, C extends Collection<I>> BeanRelationFixer<S, I> of(BiConsumer<S, C> setter, Function<S, C> getter, Supplier<C> collectionFactory) {
		return of(setter, getter, collectionFactory, (a, b) -> { /* no bi-directional relation, nothing to do */ });
	}
	
	/**
	 * Shortcut to {@link #of(BiConsumer, Function, Supplier, BiConsumer)} with a supplier that will isntanciate the given concrete Collection class
	 *
	 * @param setter the method that sets the {@link Collection} onto the target bean
	 * @param getter the method that gets the {@link Collection} from the target bean
	 * @param concreteCollectionType the Class that will be instanciated to fill the relation if it is null
	 * @return a {@link BeanRelationFixer} that will add the input to the Collection and create if the getter returns null
	 */
	static <S, I, C extends Collection<I>> BeanRelationFixer<S, I> of(BiConsumer<S, C> setter, Function<S, C> getter,
																	  Class<? extends C> concreteCollectionType,
																	  BiConsumer<I, S> reverseSetter) {
		return of(setter, getter, () -> Reflections.newInstance(concreteCollectionType), reverseSetter);
	}
	
	/**
	 * Shortcut to create a {@link BeanRelationFixer} for a bidirectionnal one-to-many relation where the attribute is a {@link Collection}
	 *
	 * @param setter the method that sets the {@link Collection} onto the target bean
	 * @param getter the method that gets the {@link Collection} from the target bean
	 * @param collectionFactory a supplier of an instance to fill the relation if it is null
	 * @param reverseSetter the setter for the other side of the relation
	 * @return a {@link BeanRelationFixer} that will add the input to the Collection and create if the getter returns null
	 */
	static <S, I, C extends Collection<I>> BeanRelationFixer<S, I> of(BiConsumer<S, C> setter, Function<S, C> getter, Supplier<C> collectionFactory,
																	  BiConsumer<I, S> reverseSetter) {
		return (target, input) -> {
			C collection = getter.apply(target);
			if (collection == null) {
				// we fill the relation
				collection = collectionFactory.get();
				setter.accept(target, collection);
			}
			collection.add(input);
			// bidirectional assignment
			reverseSetter.accept(input, target);
		};
	}
}
