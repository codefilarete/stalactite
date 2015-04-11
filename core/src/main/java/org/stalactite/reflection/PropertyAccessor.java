package org.stalactite.reflection;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Member;

import org.stalactite.lang.exception.Exceptions;

/**
 * @author mary
 */
public abstract class PropertyAccessor<C, T> {
	
	public PropertyAccessor() {
	}
	
	public abstract Member getGetter();
	
	public abstract Member getSetter();
	
	public T get(C c) throws IllegalAccessException {
		try {
			return doGet(c);
		} catch (NullPointerException npe) {
			throw new NullPointerException("Cannot access " + getGetter().toString() + " on null instance");
		} catch (InvocationTargetException e) {
			Exceptions.throwAsRuntimeException(e.getCause());
			return null;
		}
	}
	
	protected abstract T doGet(C c) throws IllegalAccessException, InvocationTargetException;
	
	public void set(C c, T t) throws IllegalAccessException {
		try {
			doSet(c, t);
		} catch (InvocationTargetException e) {
			Exceptions.throwAsRuntimeException(e.getCause());
		}
	}
	
	protected abstract void doSet(C c, T t) throws IllegalAccessException, InvocationTargetException;
}
