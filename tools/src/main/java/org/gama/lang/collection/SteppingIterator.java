package org.gama.lang.collection;

import java.util.Iterator;

/**
 * Iterator qui appelle une méthode périodiquement toutes les N itérations.
 * C'est {@link #hasNext()} qui déclenche l'appel à {@link #onStep()} afin
 * de correspondre à une boucle while(hasNext) ou foreach. Ce n'est pas {@link #next()}
 * qui déclenche l'appel car ça semble un peu tard : une incrémentation de trop.
 * {@link #onStep()} est également appelé quand {@link #hasNext()} renvoie false (sauf
 * lors du premier appel) afin de traiter le reliquat d'objet depuis le dernier
 * appel à {@link #onStep()}
 * 
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
		if (	// step reached
				stepCounter == step
				// or has remainings (end reached and not started)
				|| (!hasNext && stepCounter != 0)) {
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
