package org.gama.lang.bean;

import java.lang.reflect.Method;
import java.util.Iterator;

/**
 * Iterator over (declared) methods of a class hierarchy
 * 
 * @author Guillaume Mary
 */
public class MethodIterator extends InheritedElementIterator<Method> {
	
	public MethodIterator(Class aClass) {
		this(new ClassIterator(aClass));
	}
	
	public MethodIterator(Iterator<Class> classIterator) {
		super(classIterator);
	}
	
	/**
	 * Gives methods of a class. Default is {@link Class#getDeclaredMethods()}.
	 * Can be overriden to filter methods for instance.
	 * 
	 * @param clazz the class for which fields must be given
	 * @return an array of Field, not null
	 */
	@Override
	protected Method[] getElements(Class clazz) {
		return clazz.getDeclaredMethods();
	}
	
}
