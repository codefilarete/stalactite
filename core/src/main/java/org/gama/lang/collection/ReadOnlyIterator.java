package org.gama.lang.collection;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * @author mary
 */
public abstract class ReadOnlyIterator<E> implements Iterator<E> {
	
	@Override
	public E next() {
		if (!hasNext()) {
			throw new NoSuchElementException();
		} else {
			return getNext();
		}
	}
	
	protected abstract E getNext();
	
	@Override
	public final void remove() {
		throw new UnsupportedOperationException();
	}
}
