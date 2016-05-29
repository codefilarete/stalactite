package org.gama.lang.bean;

import java.lang.reflect.Field;

import org.gama.lang.collection.ArrayIterator;
import org.gama.lang.collection.ReadOnlyIterator;

/**
 * @author Guillaume Mary
 */
public class FieldIterator extends ReadOnlyIterator<Field> {
	
	private ClassIterator classIterator;
	private ArrayIterator<Field> fieldIterator;
	
	public FieldIterator(Class currentClass) {
		this(new ClassIterator(currentClass));
	}
	
	public FieldIterator(ClassIterator classIterator) {
		this.classIterator = classIterator;
		this.fieldIterator = new ArrayIterator<>(getFields(classIterator.next()));
	}
	
	/**
	 * Gives fields of a class. Default is {@link Class#getDeclaredFields()}.
	 * Can be overriden to filter fields for instance.
	 *
	 * @param clazz the class for which fields must be given
	 * @return an array of Field, not null
	 */
	protected Field[] getFields(Class clazz) {
		return clazz.getDeclaredFields();
	}
	
	@Override
	public boolean hasNext() {
		if (fieldIterator.hasNext()) {
			// simple case
			return true;
		} else {
			// no more field for the current iterator => we must scan upper classes if they have fields
			while (classIterator.hasNext() && !fieldIterator.hasNext()) {
				// transforming the class fields to an Iterator
				Field[] fields = getFields(classIterator.next());
				fieldIterator = new ArrayIterator<>(fields);
			}
			return fieldIterator.hasNext();
		}
	}
	
	@Override
	public Field next() {
		return fieldIterator.next();
	}
}
