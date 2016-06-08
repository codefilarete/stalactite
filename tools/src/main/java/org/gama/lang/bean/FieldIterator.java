package org.gama.lang.bean;

import java.lang.reflect.Field;
import java.util.Iterator;

/**
 * @author Guillaume Mary
 */
public class FieldIterator extends InheritedElementIterator<Field> {
	
	public FieldIterator(Class aClass) {
		this(new ClassIterator(aClass));
	}
	
	public FieldIterator(Iterator<Class> classIterator) {
		super(classIterator);
	}
	
	/**
	 * Gives fields of a class. Default is {@link Class#getDeclaredFields()}.
	 * Can be overriden to filter fields for instance.
	 *
	 * @param clazz the class for which fields must be given
	 * @return an array of Field, not null
	 */
	@Override
	protected Field[] getElements(Class clazz) {
		return clazz.getDeclaredFields();
	}
}
