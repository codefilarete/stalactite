package org.gama.lang.collection;

import java.util.*;
import java.util.Map.Entry;
import java.util.function.Function;
import java.util.function.Predicate;

import org.gama.lang.bean.Objects;

/**
 * @author Guillaume Mary
 */
public final class Iterables {
	
	/**
	 * Renvoie la première valeur d'une Iterable
	 * 
	 * @return null s'il n'y a pas de valeur dans l'Iterable
	 */
	public static <E> E first(Iterable<E> iterable) {
		return first(iterable, null);
	}
	
	public static <E> E first(Iterable<E> iterable, E defaultValue) {
		if (iterable == null) {
			return defaultValue;
		} else {
			return first(iterable.iterator());
		}
	}
	
	public static <E> E first(Iterator<E> iterator) {
		return first(iterator, null);
	}
	
	public static <E> E first(Iterator<E> iterator, E defaultValue) {
		return iterator.hasNext() ? iterator.next() : defaultValue;
	}
	
	public static <E> E first(List<E> iterable) {
		return first(iterable, null);
	}
	
	public static <E> E first(List<E> iterable, E defaultValue) {
		if (Collections.isEmpty(iterable)) {
			return defaultValue;
		} else {
			return iterable.get(0);
		}
	}
	
	public static <E> E first(E[] iterable) {
		return first(iterable, null);
	}
	
	public static <E> E first(E[] iterable, E defaultValue) {
		if (Arrays.isEmpty(iterable)) {
			return defaultValue;
		} else {
			return iterable[0];
		}
	}
	
	/**
	 * Renvoie la première entrée d'une Map, intéressant pour une {@link SortedMap} ou une {@link LinkedHashMap}
	 * 
	 * @param iterable une Map, SortedMap ou LinkedHashMap sinon ça n'a pas d'intérêt
	 * @return la première entrée de la Map, null si iterable est null
	 */
	public static <K, V> Map.Entry<K, V> first(Map<K, V> iterable) {
		return first(iterable, null);
	}
	
	public static <K, V> Map.Entry<K, V> first(Map<K, V> iterable, Map.Entry<K, V> defaultValue) {
		if (iterable == null) {
			return defaultValue;
		} else {
			return first(iterable.entrySet());
		}
	}
	
	/**
	 * Renvoie la première valeur d'une Map, intéressant pour une {@link SortedMap} ou une {@link LinkedHashMap}
	 * 
	 * @param iterable une Map, SortedMap ou LinkedHashMap sinon ça n'a pas d'intérêt
	 * @return la première valeur de la Map, null si iterable est null ou s'il n'y a pas d'entrée dans la Map
	 */
	public static <V> V firstValue(Map<?, V> iterable) {
		return firstValue(iterable, null);
	}
	
	public static <V> V firstValue(Map<?, V> iterable, V defaultValue) {
		Entry<?, V> firstEntry = first(iterable);
		return firstEntry == null ? defaultValue : firstEntry.getValue();
	}
	
	public static boolean isEmpty(Iterable iterable) {
		return iterable == null
				|| ((iterable instanceof Collection) ? ((Collection) iterable).isEmpty() : iterable.iterator().hasNext()) ;
	}
	
	public static <E> E last(List<E> iterable) {
		return last(iterable, null);
	}
	
	public static <E> E last(List<E> iterable, E defaultValue) {
		if (iterable == null || iterable.isEmpty()) {
			return defaultValue;
		} else {
			return iterable.get(iterable.size()-1);
		}
	}
	
	/**
	 * Crée une copie du Iterable en tant que List
	 * @param iterable un Iterable, non null
	 * @return une List<E> qui contient tous les éléments de <t>iterable</t>
	 */
	public static <E> List<E> copy(Iterable<E> iterable) {
		return (iterable instanceof Collection) ? new ArrayList<>((Collection<E>) iterable) : copy(iterable.iterator());
	}
	
	/**
	 * Crée une copie du Iterator en tant que List
	 * @param iterator un Iterator, non null
	 * @return une List<E> qui contient tous les éléments de <t>iterator</t>
	 */
	public static <E> List<E> copy(Iterator<E> iterator) {
		return copy(iterator, new ArrayList<>());
	}
	
	/**
	 * Ajoute tous les élément de l'Iterator dans une List
	 * @param iterator un Iterator, non null
	 * @param result the target of the copy
	 * @return the result instance
	 */
	public static <E> List<E> copy(Iterator<E> iterator, List<E> result) {
		while (iterator.hasNext()) {
			result.add(iterator.next());
		}
		return result;
	}
	
	public static <E> Iterable<E> filter(Iterable<E> iterable, Objects.Predicate<E> predicate) {
		Iterator<E> iterator = iterable.iterator();
		while (iterator.hasNext()) {
			E e = iterator.next();
			if (!predicate.test(e)) {
				iterator.remove();
			}
			
		}
		return iterable;
	}
	
	/**
	 * Find the first non null value (according to mapper) into the {@link Iterator} of an {@link Iterable}
	 * 
	 * @param iterable the {@link Iterable} to scan
	 * @param mapper the mapper to extract the value to test
	 * @param <I> input type
	 * @param <O> output type
	 * @return null if all mapped values are null
	 */
	public static <I, O> O find(Iterable<I> iterable, Function<I, O> mapper) {
		return find(iterable.iterator(), mapper, java.util.Objects::isNull);
	}
	
	/**
	 * Find the first non null value (according to mapper) into an {@link Iterator}
	 * 
	 * @param iterator the {@link Iterator} to scan
	 * @param mapper the mapper to extract the value to test
	 * @param <I> input type
	 * @param <O> output type
	 * @return null if all mapped values are null
	 */
	public static <I, O> O find(Iterator<I> iterator, Function<I, O> mapper) {
		return find(iterator, mapper, java.util.Objects::nonNull);
	}
	
	/**
	 * Find the first predicate-matching value (according to mapper) into the {@link Iterator} of an {@link Iterable}
	 *
	 * @param iterable the {@link Iterable} to scan
	 * @param mapper the mapper to extract the value to test
	 * @param <I> input type
	 * @param <O> output type
	 * @return null if no mapped values matches the predicate
	 */
	public static <I, O> O find(Iterable<I> iterable, Function<I, O> mapper, Predicate<O> predicate) {
		return find(iterable.iterator(), mapper, predicate);
	}
	
	/**
	 * Find the first predicate-matching value (according to mapper) into an {@link Iterator}
	 * 
	 * @param iterator the {@link Iterator} to scan
	 * @param mapper the mapper to extract the value to test
	 * @param <I> input type
	 * @param <O> output type
	 * @return null if no mapped values matches the predicate
	 */
	public static <I, O> O find(Iterator<I> iterator, Function<I, O> mapper, Predicate<O> predicate) {
		O foundObject = null;
		boolean found = false;
		while (iterator.hasNext() && !found) {
			I step = iterator.next();
			O mapperResult = mapper.apply(step);
			if (found = predicate.test(mapperResult)) {
				foundObject = mapperResult;
			}
		}
		return foundObject;
	}
	
	public static <E> Iterator<E> reverseIterator(List<E> list) {
		return new ReverseListIterator<>(list);
	}
	
	/**
	 * Visite chaque élément de <i>c</i> pour y appliquer <i>visitor</i>
	 * @param c une collection au sens large, null accepté, dans ce cas ne fait rien
	 * @param visitor un visiteur de collection
	 * @return le résultat de la visite de <i>c</i> par <i>visitor</i>, null si <i>c</i> est null   
	 */
	public static <E, O> List<O> visit(Iterable<E> c, IVisitor<E, O> visitor) {
		if (c != null) {
			Iterator<E> iterator = c.iterator();
			return visit(iterator, visitor);
		} else {
			return null;
		}
	}
	
	/**
	 * Visite chaque élément de <i>iterator</i> pour y appliquer <i>visitor</i>
	 * @param iterator une collection au sens large, null accepté, dans ce cas ne fait rien
	 * @param visitor un visiteur de collection
	 * @return le résultat de la visite de <i>iterator</i> par <i>visitor</i>, null si <i>iterator</i> est null   
	 */
	public static <E, O> List<O> visit(Iterator<E> iterator, IVisitor<E, O> visitor) {
		List<O> result = new ArrayList<>();
		boolean pursue = true;
		while (pursue && iterator.hasNext()) {
			E next = iterator.next();
			result.add(visitor.visit(next));
			pursue = visitor.pursue();
		}
		return result;
	}
	
	/**
	 * Visite chaque élément de <i>c</i> pour y appliquer <i>visitor</i> afin de ramener un résultat
	 * @param c une collection au sens large, null accepté, dans ce cas ne fait rien
	 * @param visitor un visiteur de collection
	 * @return le résultat alimenté par le <i>visitor</i> (null sans doute si c est null)
	 */
	public static <E, O> O filter(Iterable<E> c, IResultVisitor<E, O> visitor) {
		if (c != null) {
			visit(c, visitor);
			return visitor.getResult();
		} else {
			return null;
		}
	}

	/**
	 * Visite chaque élément de <i>iterator</i> pour y appliquer <i>visitor</i> afin de ramener un résultat
	 * @param iterator une collection au sens large, null accepté, dans ce cas ne fait rien
	 * @param visitor un visiteur de collection
	 * @return le résultat alimenté par le <i>visitor</i> (null sans doute si iterator est null)
	 */
	public static <E, O> O filter(Iterator<E> iterator, IResultVisitor<E, O> visitor) {
		visit(iterator, visitor);
		return visitor.getResult();
	}
	
	public interface IVisitor<E, R> {
		R visit(E e);
		
		boolean pursue();
	}
	
	/**
	 * Visiteur utilisé pour ceux qui doivent parcourir tous les éléments d'un Iterator
	 * @param <E>
	 * @param <R>
	 */
	public static abstract class ForEach<E, R> implements IVisitor<E, R> {
		@Override
		public final boolean pursue() {
			return true;
		}
	}
	
	/**
	 * Parent de tous les IVisitor qui ramène un résultat
	 * @param <E>
	 * @param <O>
	 */
	public interface IResultVisitor<E, O> extends IVisitor<E, O> {
		O getResult();
	}
	
	private abstract static class AbstractFilter<E, T> implements IResultVisitor<E, T> {

		protected T foundElements;
		
		@Override
		public T visit(E e) {
			boolean accept = accept(e);
			if (accept) {
				onAccepted(e);
			}
			return foundElements;
		}
		
		@Override
		public T getResult() {
			return foundElements;
		}
		
		protected abstract void onAccepted(E e);
		
		public abstract boolean accept(E e);
	}
	
	/**
	 * Visiteur dédié à la recherche de plusieurs éléments dans un Iterator. Parcourt tous les éléments de l'Iterator.
	 * 
	 * @param <E>
	 */
	public abstract static class Filter<E> extends AbstractFilter<E, List<E>> {
		
		public Filter() {
			super.foundElements = new ArrayList<>();
		}
		
		@Override
		public final boolean pursue() {
			return true;
		}
		
		@Override
		protected final void onAccepted(E e) {
			this.foundElements.add(e);
		}
	}
	
	/**
	 * Visiteur dédié à la recherche d'un élément dans un Iterator. S'arrête dès que l'objet est trouvé.
	 * 
	 * @param <E>
	 */
	public abstract static class Finder<E> extends AbstractFilter<E, E> {

		private boolean pursue = true;
		
		@Override
		public final boolean pursue() {
			return pursue;
		}
		
		@Override
		protected final void onAccepted(E e) {
			this.foundElements = e;
			this.pursue = false;
		}
	}
	
	/**
	 * Visiteur dédié à la récupération d'une propriété de chaque élément d'un Iterator. Le résultat est une Map
	 * de chaque valeur vers chaque élément de l'Iterator parcouru
	 * 
	 * @param <K>
	 * @param <E>
	 */
	public abstract static class Mapper<K, E> implements IResultVisitor<E, Map<K, E>> {

		protected  Map<K, E> foundElements;
		
		public Mapper(Map<K, E> resultContainer) {
			this.foundElements = resultContainer;
		}
		
		@Override
		public Map<K, E> visit(E e) {
			boolean accept = accept(e);
			if (accept) {
				onAccepted(e);
			}
			return foundElements;
		}
		
		@Override
		public Map<K, E> getResult() {
			return foundElements;
		}
		
		protected void onAccepted(E e) {
			foundElements.put(getKey(e), e);
		}
		
		@Override
		public boolean pursue() {
			return true;
		}
		
		public boolean accept(E e) {
			return true;
		}
		
		protected abstract K getKey(E e);
	}
	
	private Iterables() {}
}
