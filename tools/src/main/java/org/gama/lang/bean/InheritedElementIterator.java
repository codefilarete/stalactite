package org.gama.lang.bean;

import org.gama.lang.collection.ArrayIterator;
import org.gama.lang.collection.ReadOnlyIterator;

/**
 * @author Guillaume Mary
 */
public abstract class InheritedElementIterator<T> extends ReadOnlyIterator<T> {
	
	private ClassIterator classIterator;
	private ArrayIterator<T> inheritedElementIterator;
	
	public InheritedElementIterator(ClassIterator classIterator) {
		this.classIterator = classIterator;
	}
	
	@Override
	public boolean hasNext() {
		// take first time into account
		if (inheritedElementIterator == null) {
			this.inheritedElementIterator = new ArrayIterator<>(getElements(classIterator.next()));
		}
		// simple case
		if (inheritedElementIterator.hasNext()) {
			return true;
		} else {
			// no more element for the current iterator => we must scan upper classes if they have some
			while (classIterator.hasNext() && !inheritedElementIterator.hasNext()) {
				// transforming the class elements to an Iterator
				T[] methods = getElements(classIterator.next());
				inheritedElementIterator = new ArrayIterator<>(methods);
			}
			return inheritedElementIterator.hasNext();
		}
	}
	
	@Override
	public T next() {
		return inheritedElementIterator.next();
	}
	
	/**
	 * Gives any element from a class: field, method, interface, ...
	 *
	 * @param clazz the class for which elements must be given
	 * @return an array of element, not null
	 */
	protected abstract T[] getElements(Class clazz);
}
