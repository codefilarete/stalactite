package org.gama.lang.bean;

import java.util.Collections;
import java.util.Iterator;

import org.gama.lang.collection.ArrayIterator;
import org.gama.lang.collection.ReadOnlyIterator;

/**
 * An {@link java.util.Iterator} that gets its informations from each class of a hierarchy.
 * 
 * @author Guillaume Mary
 */
public abstract class InheritedElementIterator<T> extends ReadOnlyIterator<T> {
	
	protected Iterator<Class> classIterator;
	protected Iterator<T> inheritedElementIterator = Collections.emptyIterator();
	
	public InheritedElementIterator(Class aClass) {
		this(new ClassIterator(aClass));
	}
	
	public InheritedElementIterator(Iterator<Class> classIterator) {
		this.classIterator = classIterator;
	}
	
	@Override
	public boolean hasNext() {
		// simple case
		if (inheritedElementIterator.hasNext()) {
			return true;
		} else {
			// no more element for the current iterator => we must scan upper classes if they have some
			while (classIterator.hasNext() && !inheritedElementIterator.hasNext()) {
				// transforming the class elements to an Iterator
				inheritedElementIterator = nextInheritedElementIterator(classIterator.next());
			}
			return inheritedElementIterator.hasNext();
		}
	}
	
	protected Iterator<T> nextInheritedElementIterator(Class clazz) {
		return new ArrayIterator<>(getElements(clazz));
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
