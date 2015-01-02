package org.stalactite.lang.collection;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;

/**
 * @author mary
 */
public class Iterables {
	
	/**
	 * Renvoie la première valeur d'une Iterable
	 * 
	 * @return null s'il n'y a pas de valeur dans l'Iterable
	 */
	public static <E> E first(Iterable<E> iterable) {
		if (iterable == null) {
			return null;
		} else {
			Iterator<E> iterator = iterable.iterator();
			return iterator.hasNext() ? iterator.next() : null;
		}
	}
	
	/**
	 * Renvoie la première entrée d'une Map, intéressant pour une {@link SortedMap} ou une {@link LinkedHashMap}
	 * 
	 * @param iterable une Map, SortedMap ou LinkedHashMap sinon ça n'a pas d'intérêt
	 * @return la première entrée de la Map, null si iterable est null
	 */
	public static <K, V> Map.Entry<K, V> first(Map<K, V> iterable) {
		if (iterable == null) {
			return null;
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
		Entry<?, V> firstEntry = first(iterable);
		return firstEntry == null ? null : firstEntry.getValue();
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
	public static <E, O> O find(Iterable<E> c, IResultVisitor<E, O> visitor) {
		visit(c, visitor);
		return visitor.getResult();
	}

	public static interface IVisitor<E, R> {
		R visit(E e);
		
		boolean pursue();
	}
	
	public static abstract class ForEach<E, R> implements IVisitor<E, R> {
		@Override
		public final boolean pursue() {
			return true;
		}
	}
	
	public static interface IResultVisitor<E, O> extends IVisitor<E, O> {
		O getResult();
	}

	public abstract static class Finder<E> implements IResultVisitor<E, E> {

		private E foundElement;
		private boolean pursue = true;
		
		@Override
		public E visit(E e) {
			boolean accept = accept(e);
			if (accept) {
				this.foundElement = e;
				this.pursue = false;
			}
			return foundElement;
		}

		@Override
		public E getResult() {
			return foundElement;
		}

		public abstract boolean accept(E e);
	}
}
