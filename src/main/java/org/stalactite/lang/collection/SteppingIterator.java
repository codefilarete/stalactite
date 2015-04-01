package org.stalactite.lang.collection;

import java.util.Iterator;

/**
 * @author Guillaume Mary
 */
public abstract class SteppingIterator<E> implements Iterator<E> {
	
	private final Iterator<E> delegate;
	private long stepCounter = 0;
	private final long step;
	
	public SteppingIterator(Iterable<E> delegate, long step) {
		this(delegate.iterator(), step);
	}
	
	public SteppingIterator(Iterator<E> delegate, long step) {
		this.delegate = delegate;
		this.step = step;
	}
	
	@Override
	public boolean hasNext() {
		boolean hasNext = delegate.hasNext();
		if (stepCounter == step || (!hasNext && stepCounter != 0)) {
			onStep();
			stepCounter = 0;
		}
		return hasNext;
	}
	
	@Override
	public E next() {
		stepCounter++;
		return delegate.next();
	}
	
	protected abstract void onStep();
	
	@Override
	public void remove() {
		delegate.remove();
	}
}
