package org.stalactite.reflection;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * @author mary
 */
public class AccessorByMethod<C, T> extends PropertyAccessor<C, T> {
	
	private final Method getter, setter;
	
	public AccessorByMethod(Method getter, Method setter) {
		super();
		this.getter = getter;
		this.setter = setter;
		this.getter.setAccessible(true);
		this.setter.setAccessible(true);
	}
	
	public Method getGetter() {
		return getter;
	}
	
	public Method getSetter() {
		return setter;
	}
	
	@Override
	protected T doGet(C c) throws IllegalAccessException, InvocationTargetException {
		return (T) getGetter().invoke(c);
	}
	
	@Override
	protected void doSet(C c, T t) throws IllegalAccessException, InvocationTargetException {
		getSetter().invoke(c, t);
	}
}
