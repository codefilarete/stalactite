package org.stalactite.lang.collection;

/**
 * Encapsule un tableau dans un Iterator. Permet la réutilisation d'un tableau dans l'API des Iterators.
 * 
 * @author mary
 */
public class ArrayIterator<O> extends ReadOnlyIterator<O> {

	private O[] array;
	private int currentIndex = 0, maxIndex;

	public ArrayIterator(O[] array) {
		this.array = array;
		this.maxIndex = array.length;
	}

	@Override
	public boolean hasNext() {
		return currentIndex < maxIndex;
	}

	@Override
	protected O getNext() {
		return this.array[currentIndex++];
	}
}
