package org.gama.lang.bean;

import java.lang.reflect.Method;

import org.gama.lang.collection.ArrayIterator;
import org.gama.lang.collection.ReadOnlyIterator;

/**
 * Iterator over (declared) methods of a class hierarchy
 * 
 * @author Guillaume Mary
 */
public class MethodIterator extends ReadOnlyIterator<Method> {
	
	private ClassIterator classIterator;
	private ArrayIterator<Method> methodIterator;
	
	public MethodIterator(Class currentClass) {
		this(new ClassIterator(currentClass));
	}
	
	public MethodIterator(ClassIterator classIterator) {
		this.classIterator = classIterator;
		this.methodIterator = new ArrayIterator<>(getMethods(classIterator.next()));
	}
	
	/**
	 * Gives methods of a class. Default is {@link Class#getDeclaredMethods()}.
	 * Can be overriden to filter methods for instance.
	 * 
	 * @param clazz the class for which fields must be given
	 * @return an array of Field, not null
	 */
	protected Method[] getMethods(Class clazz) {
		return clazz.getDeclaredMethods();
	}
	
	@Override
	public boolean hasNext() {
		if (methodIterator.hasNext()) {
			return true;
		} else {
			// no more method for the current iterator => we must scan upper classes if they have methods
			while (classIterator.hasNext() && !methodIterator.hasNext()) {
				// transforming the class fields to an Iterator
				Method[] methods = getMethods(classIterator.next());
				methodIterator = new ArrayIterator<>(methods);
			}
			return methodIterator.hasNext();
		}
	}
	
	@Override
	public Method next() {
		return methodIterator.next();
	}
}
