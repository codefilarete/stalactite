package org.gama.lang.collection;

import java.util.Iterator;

/**
 * @author Guillaume Mary
 */
public abstract class ReadOnlyIterator<E> implements Iterator<E> {
	
	@Override
	public final void remove() {
		throw new UnsupportedOperationException();
	}
}
