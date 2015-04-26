package org.stalactite.reflection;

import java.lang.reflect.InvocationTargetException;

import org.stalactite.lang.exception.Exceptions;

/**
 * @author Guillaume Mary
 */
public abstract class AbstractAccessor<C, T> implements IAccessor<C, T> {
	
	@Override
	public T get(C c) throws IllegalAccessException {
		try {
			return doGet(c);
		} catch (NullPointerException npe) {
			throw new NullPointerException("Cannot call " + getGetterDescription() + " on null instance");
		} catch (InvocationTargetException e) {
			Exceptions.throwAsRuntimeException(e.getCause());
			return null;
		}
	}
	
	protected abstract T doGet(C c) throws IllegalAccessException, InvocationTargetException;
	
	protected abstract String getGetterDescription();
}
