package org.codefilarete.stalactite.sql.result;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;

import org.codefilarete.tool.Reflections;
import org.codefilarete.tool.function.TriConsumer;

/**
 * An interface aimed at abstracting the way how relations between 2 beans are filled : implementation should handle one-to-one relationship
 * as well as one-to-many relationship.
 * Since implementations are quite simple, they are done through all "of" static methods in this interface.
 * 
 * @param <E> bean type on which relation must be applied to
 * @param <I> relation input type 
 *     
 * @author Guillaume Mary
 * @see #of(BiConsumer, Function, Class)
 * @see #of(BiConsumer, Function, Supplier)
 * @see #of(BiConsumer)
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
	static <E, I> BeanRelationFixer<E, I> of(BiConsumer<E, I> setter) {
		return of(setter, (a, b) -> { /* no bi-directional relation, nothing to do */ });
	}
	
	/**
	 * Shortcut to create a {@link BeanRelationFixer} for a bidirectional one-to-one relation.
	 *
	 * @param setter the method that fixes the relation
	 * @param reverseSetter the setter for the other side of the relation   
	 * @return a {@link BeanRelationFixer} mapped to {@link BiConsumer#accept(Object, Object)}
	 */
	static <E, I> BeanRelationFixer<E, I> of(BiConsumer<E, I> setter, BiConsumer<I, E> reverseSetter) {
		return (s, i) -> {
			setter.accept(s, i);
			// bidirectional assignment
			reverseSetter.accept(i, s);
		};
	}
	
	/**
	 * Returns a {@link Supplier} of concrete instance for given collection type : for {@link List} and {@link Set} types it respectively
	 * returns an {@link ArrayList} instance and an {@link HashSet} instance, for any other case collectionType is expected to be concrete therefore
	 * it will try to instantiate it.
	 * 
	 * @param collectionType expected to be one of List.class or Set.class or a concrete type
	 * @return a {@link Supplier} of a concrete {@link Collection} compatible with given collectionType
	 */
	static <C extends Collection> Supplier<C> giveCollectionFactory(Class<C> collectionType) {
		Class<? extends C> concreteType;
		if (List.class.equals(collectionType)) {
			concreteType = (Class) ArrayList.class;
		} else if (SortedSet.class.equals(collectionType)) {
			concreteType = (Class) TreeSet.class;
		} else if (Set.class.equals(collectionType)) {
			concreteType = (Class) HashSet.class;
		} else if (Queue.class.equals(collectionType)) {
			concreteType = (Class) ArrayDeque.class;
		} else {
			// given type is expected to be concrete, we'll instantiate it
			concreteType = collectionType;
		}
		return () -> Reflections.newInstance(concreteType);
	}
	
	static <M extends Map> Supplier<M> giveMapFactory(Class<M> mapType) {
		Class<? extends M> concreteType;
		if (SortedMap.class.equals(mapType)) {
			concreteType = (Class) TreeMap.class;
		} else if (Map.class.equals(mapType)) {
			concreteType = (Class) HashMap.class;
		} else {
			// given type is expected to be concrete, we'll instantiate it
			concreteType = mapType;
		}
		return () -> Reflections.newInstance(concreteType);
	}
	
	/**
	 * Shortcut to {@link #of(BiConsumer, Function, Supplier)} with a supplier that will instantiate the given concrete Collection class
	 * 
	 * @param setter the method that sets the {@link Collection} onto the target bean
	 * @param getter the method that gets the {@link Collection} from the target bean
	 * @param concreteCollectionType the Class that will be instanced to fill the relation if it is null
	 * @return a {@link BeanRelationFixer} that will add the input to the Collection and create if the getter returns null
	 */
	static <E, I, C extends Collection<I>> BeanRelationFixer<E, I> of(BiConsumer<E, C> setter, Function<E, C> getter,
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
	static <E, I, C extends Collection<I>> BeanRelationFixer<E, I> of(BiConsumer<E, C> setter, Function<E, C> getter, Supplier<C> collectionFactory) {
		return of(setter, getter, collectionFactory, (a, b) -> { /* no bi-directional relation, nothing to do */ });
	}
	
	/**
	 * Shortcut to {@link #of(BiConsumer, Function, Supplier, BiConsumer)} with a supplier that will instantiate the given concrete Collection class
	 *
	 * @param setter the method that sets the {@link Collection} onto the target bean
	 * @param getter the method that gets the {@link Collection} from the target bean
	 * @param concreteCollectionType the Class that will be instanced to fill the relation if it is null
	 * @return a {@link BeanRelationFixer} that will add the input to the Collection and create if the getter returns null
	 */
	static <E, I, C extends Collection<I>> BeanRelationFixer<E, I> of(BiConsumer<E, C> setter, Function<E, C> getter,
																	  Class<? extends C> concreteCollectionType,
																	  BiConsumer<I, E> reverseSetter) {
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
	static <E, I, C extends Collection<I>> BeanRelationFixer<E, I> of(BiConsumer<E, C> setter, Function<E, C> getter, Supplier<C> collectionFactory,
																	  BiConsumer<I, E> reverseSetter) {
		return ofAdapter(setter, getter, collectionFactory, (target, input, collection) -> {
			collection.add(input);
			// bidirectional assignment
			reverseSetter.accept(input, target);
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
	static <E, I, C extends Collection<?>> BeanRelationFixer<E, I> ofAdapter(BiConsumer<E, C> setter,
																			 Function<E, C> getter,
																			 Supplier<C> collectionFactory,
																			 TriConsumer<E, I, C> adapter) {
		return (target, input) -> {
			C collection = getter.apply(target);
			if (collection == null) {
				// we fill the relation
				collection = collectionFactory.get();
				setter.accept(target, collection);
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
