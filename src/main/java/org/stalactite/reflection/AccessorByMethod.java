package org.stalactite.reflection;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * @author mary
 */
public class AccessorByMethod<C, T> extends PropertyAccessor<C, T, Method> {
	
	private final Class<T> returnType;
	
	public AccessorByMethod(Method method) {
		super(method);
		method.setAccessible(true);
		this.returnType = (Class<T>) getAccessor().getReturnType();
	}
	
	@Override
	public T get(C c) throws IllegalAccessException, InvocationTargetException {
		try {
			return (T) getAccessor().invoke(c);
		} catch (NullPointerException npe) {
			throw new NullPointerException("Cannot access " + getAccessor().toString() + " on null instance");
		}
	}
	
	@Override
	public void set(C c, T t) throws IllegalAccessException, InvocationTargetException {
		getAccessor().invoke(c, t);
	}
}
