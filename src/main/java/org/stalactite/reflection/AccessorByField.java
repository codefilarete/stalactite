package org.stalactite.reflection;

import java.lang.reflect.Field;

/**
 * @author mary
 */
public class AccessorByField<C, T> extends PropertyAccessor<C, T, Field> {
	
	private final Class<T> returnType;
	
	public AccessorByField(Field field) {
		super(field);
		field.setAccessible(true);
		this.returnType = (Class<T>) field.getType();
	}
	
	@Override
	public T get(C c) throws IllegalAccessException {
		try {
			return returnType.cast(getAccessor().get(c));
		} catch (NullPointerException npe) {
			throw new NullPointerException("Cannot access " + getAccessor().toString() + " on null instance");
		}
	}
	
	@Override
	public void set(C c, T t) throws IllegalAccessException {
		getAccessor().set(c, t);
	}
}
