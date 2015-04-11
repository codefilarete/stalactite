package org.stalactite.reflection;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;

/**
 * @author mary
 */
public class AccessorByField<C, T> extends PropertyAccessor<C, T> {
	
	private final Field field;
	
	public AccessorByField(Field field) {
		super();
		this.field = field;
		this.field.setAccessible(true);
	}
	
	@Override
	public Field getGetter() {
		return field;
	}
	
	@Override
	public Field getSetter() {
		return field;
	}
	
	@Override
	protected T doGet(C c) throws IllegalAccessException, InvocationTargetException {
		return (T) getGetter().get(c);
	}
	
	@Override
	protected void doSet(C c, T t) throws IllegalAccessException, InvocationTargetException {
		getGetter().set(c, t);
	}
}
