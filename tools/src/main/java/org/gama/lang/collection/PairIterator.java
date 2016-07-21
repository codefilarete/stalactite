package org.gama.lang.collection;

import java.util.AbstractMap.SimpleEntry;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.NoSuchElementException;

/**
 * Iterator de 2 autres sous la forme de {@link Entry}. A des éléments tant que les 2 Iterators délégués ont des élements.
 * 
 * @param <K>
 * @param <V>
 * @author mary
 */
public class PairIterator<K, V> implements Iterator<Entry<K, V>> {
	
	protected Iterator<K> iterator1;
	protected Iterator<V> iterator2;
	
	public PairIterator(Iterable<K> iterator1, Iterable<V> iterator2) {
		this(iterator1.iterator(), iterator2.iterator());
	}
	
	public PairIterator(Iterator<K> iterator1, Iterator<V> iterator2) {
		this.iterator1 = iterator1;
		this.iterator2 = iterator2;
	}
	
	@Override
	public boolean hasNext() {
		return iterator1.hasNext() && iterator2.hasNext();
	}
	
	@Override
	public Entry<K, V> next() {
		return new SimpleEntry<>(iterator1.next(), iterator2.next());
	}
	
	@Override
	public void remove() {
		iterator1.remove();
		iterator2.remove();
	}
	
	/**
	 * Iterator d'une paire qui continue tant que au moins un des 2 Iterators délégués a encore des éléments.
	 * S'arrête donc quand les 2 Iterators sont épuisés.
	 * Renvoie la paire sous la forme d'une {@link Entry}.
	 * 
	 * @param <K>
	 * @param <V>
	 */
	public static class UntilBothIterator<K, V> extends PairIterator<K, V> {
		
		public UntilBothIterator(Iterable<K> iterator1, Iterable<V> iterator2) {
			super(iterator1, iterator2);
		}
		
		public UntilBothIterator(Iterator<K> iterator1, Iterator<V> iterator2) {
			super(iterator1, iterator2);
		}
		
		@Override
		public boolean hasNext() {
			return iterator1.hasNext() || iterator2.hasNext();
		}
		
		@Override
		public Entry<K, V> next() {
			K val1;
			if (iterator1.hasNext()) {
				val1 = iterator1.next();
			} else {
				val1 = getMissingKey();
			}
			V val2;
			if (iterator2.hasNext()) {
				val2 = iterator2.next();
			} else {
				val2 = getMissingValue();
			}
			return new SimpleEntry<>(val1, val2);
		}
		
		public K getMissingKey() {
			return null;
		}
		
		public V getMissingValue() {
			return null;
		}
	}
	
	/**
	 * Iterator à l'infini, commence par renvoyer les éléments d'un Iterator délégué, puis renvoie ce que {@link #getMissingElement()}
	 * renvoie (null par défaut).
	 * 
	 * @param <E>
	 */
	public static class InfiniteIterator<E> implements Iterator<E> {
		
		private Iterator<E> delegate;
		
		public InfiniteIterator(Iterator<E> delegate) {
			this.delegate = delegate;
		}
		
		@Override
		public boolean hasNext() {
			return true;
		}
		
		@Override
		public E next() {
			try {
				return delegate.next();
			} catch (NoSuchElementException e) {
				return getMissingElement();
			}
		}
		
		@Override
		public void remove() {
			delegate.remove();
		}
		
		public E getMissingElement() {
			return null;
		}
	}
	
	/**
	 * Itérateur qui n'a aucun élément. Simple bouchon pour les API qui ont besoin d'un Iterator.
	 * @param <E>
	 */
	public static class EmptyIterator<E> implements Iterator<E> {
		
		@Override
		public boolean hasNext() {
			return false;
		}
		
		@Override
		public E next() {
			throw new NoSuchElementException();
		}
		
		@Override
		public void remove() {
			
		}
	}
}
