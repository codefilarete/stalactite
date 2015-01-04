package org.stalactite.lang.collection;

import java.util.AbstractMap.SimpleEntry;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.NoSuchElementException;

import javax.annotation.Nonnull;

/**
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
	
	public static class InfiniteIterator<E> implements Iterator<E> {
		
		private Iterator<E> delegate;
		
		public InfiniteIterator(@Nonnull Iterator<E> delegate) {
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
}
