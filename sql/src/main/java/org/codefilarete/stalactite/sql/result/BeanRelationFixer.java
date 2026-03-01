package org.codefilarete.stalactite.sql.result;

import java.util.Collection;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;

import org.codefilarete.reflection.Accessor;
import org.codefilarete.reflection.Mutator;
import org.codefilarete.tool.Reflections;
import org.codefilarete.tool.function.TriConsumer;

/**
 * An interface aimed at abstracting the way how relations between 2 beans are filled : implementation should handle one-to-one relation
 * as well as one-to-many relation.
 * Since implementations are quite simple, they are done through all "of" static methods in this interface.
 * 
 * @param <E> bean type on which relation must be applied to
 * @param <I> relation input type 
 *     
 * @author Guillaume Mary
 * @see #of(Mutator, Accessor, Class)
 * @see #of(Mutator, Accessor, Supplier)
 * @see #of(Mutator)
 */
@FunctionalInterface
public interface BeanRelationFixer<E, I> {
	
	/**
	 * Main method that fills the relation
	 * 
	 * @param target the owner of the relation
	 * @param input the objet to be written/added into the relation
	 */
	void apply(E target, I input);
	
	/**
	 * Shortcut to create a {@link BeanRelationFixer} for a one-to-one relation.
	 * 
	 * @param setter the method that fixes the relation
	 * @return a {@link BeanRelationFixer} mapped to {@link BiConsumer#accept(Object, Object)}
	 */
	static <E, I> BeanRelationFixer<E, I> of(Mutator<E, I> setter) {
		return of(setter, (a, b) -> { /* no bi-directional relation, nothing to do */ });
	}
	
	/**
	 * Shortcut to create a {@link BeanRelationFixer} for a bidirectional one-to-one relation.
	 *
	 * @param setter the method that fixes the relation
	 * @param reverseSetter the setter for the other side of the relation   
	 * @return a {@link BeanRelationFixer} mapped to {@link BiConsumer#accept(Object, Object)}
	 */
	static <E, I> BeanRelationFixer<E, I> of(Mutator<E, I> setter, Mutator<I, E> reverseSetter) {
		return (s, i) -> {
			setter.set(s, i);
			// bidirectional assignment
			reverseSetter.set(i, s);
		};
	}
	
	/**
	 * Shortcut to {@link #of(Mutator, Accessor, Supplier)} with a supplier that will instantiate the given concrete Collection class
	 * 
	 * @param setter the method that sets the {@link Collection} onto the target bean
	 * @param getter the method that gets the {@link Collection} from the target bean
	 * @param concreteCollectionType the Class that will be instanced to fill the relation if it is null
	 * @return a {@link BeanRelationFixer} that will add the input to the Collection and create if the getter returns null
	 */
	static <E, I, C extends Collection<I>> BeanRelationFixer<E, I> of(Mutator<E, C> setter,
																	  Accessor<E, C> getter,
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
	static <E, I, C extends Collection<I>> BeanRelationFixer<E, I> of(Mutator<E, C> setter, Accessor<E, C> getter, Supplier<C> collectionFactory) {
		return of(setter, getter, collectionFactory, (a, b) -> { /* no bi-directional relation, nothing to do */ });
	}
	
	/**
	 * Shortcut to {@link #of(Mutator, Accessor, Supplier, Mutator)} with a supplier that will instantiate the given concrete Collection class
	 *
	 * @param setter the method that sets the {@link Collection} onto the target bean
	 * @param getter the method that gets the {@link Collection} from the target bean
	 * @param concreteCollectionType the Class that will be instanced to fill the relation if it is null
	 * @return a {@link BeanRelationFixer} that will add the input to the Collection and create if the getter returns null
	 */
	static <E, I, C extends Collection<I>> BeanRelationFixer<E, I> of(Mutator<E, C> setter,
																	  Accessor<E, C> getter,
																	  Class<? extends C> concreteCollectionType,
																	  Mutator<I, E> reverseSetter) {
		return of(setter, getter, () -> Reflections.newInstance(concreteCollectionType), reverseSetter);
	}
	
	/**
	 * Shortcut to create a {@link BeanRelationFixer} for a bidirectionnal relation where the attribute is a {@link Collection}
	 *
	 * @param setter the method that sets the {@link Collection} onto the target bean
	 * @param getter the method that gets the {@link Collection} from the target bean
	 * @param collectionFactory a supplier of an instance to fill the relation if it is null
	 * @param reverseSetter the setter for the other side of the relation
	 * @return a {@link BeanRelationFixer} that will add the input to the Collection and create if the getter returns null
	 */
	static <E, I, C extends Collection<I>> BeanRelationFixer<E, I> of(Mutator<E, C> setter, Accessor<E, C> getter, Supplier<C> collectionFactory,
																	  Mutator<I, E> reverseSetter) {
		return ofAdapter(setter, getter, collectionFactory, (target, input, collection) -> {
			collection.add(input);
			// bidirectional assignment
			reverseSetter.set(input, target);
		});
	}
	
	/**
	 * Shortcut to create a {@link BeanRelationFixer} for a relation where the attribute is a {@link Collection}
	 *
	 * @param setter the method that sets the {@link Collection} onto the target bean
	 * @param getter the method that gets the {@link Collection} from the target bean
	 * @param collectionFactory a supplier of an instance to fill the relation if it is null
	 * @param adapter the final method that will be applied to bean, input and collection, expected to have at least a collection add, with eventual input adaptation
	 * @return a {@link BeanRelationFixer} that will add the input to the Collection and create if the getter returns null
	 */
	static <E, I, C extends Collection<?>> BeanRelationFixer<E, I> ofAdapter(Mutator<E, C> setter,
																			 Accessor<E, C> getter,
																			 Supplier<C> collectionFactory,
																			 TriConsumer<E, I, C> adapter) {
		return (target, input) -> {
			C collection = getter.get(target);
			if (collection == null) {
				// we fill the relation
				collection = collectionFactory.get();
				setter.set(target, collection);
			}
			adapter.accept(target, input, collection);
		};
	}
	
	/**
	 * Shortcut to create a {@link BeanRelationFixer} for a relation where the attribute is a {@link Map}
	 *
	 * @param setter the method that sets the {@link Map} onto the target bean
	 * @param getter the method that gets the {@link Map} from the target bean
	 * @param mapFactory a supplier of an instance to fill the relation if it is null
	 * @param adapter the final method that will be applied to bean, input and collection, expected to have at least a collection add, with eventual input adaptation
	 * @return a {@link BeanRelationFixer} that will add the input to the Collection and create if the getter returns null
	 */
	static <E, I, M extends Map<?, ?>> BeanRelationFixer<E, I> ofMapAdapter(BiConsumer<E, M> setter,
																			Function<E, M> getter,
																			Supplier<M> mapFactory,
																			TriConsumer<E, I, M> adapter) {
		return (target, input) -> {
			M map = getter.apply(target);
			if (map == null) {
				// we fill the relation
				map = mapFactory.get();
				setter.accept(target, map);
			}
			adapter.accept(target, input, map);
		};
	}
}
