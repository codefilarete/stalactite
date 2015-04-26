package org.stalactite.reflection;

import java.lang.reflect.InvocationTargetException;

import org.stalactite.lang.exception.Exceptions;

/**
 * @author Guillaume Mary
 */
public abstract class AbstractMutator<C, T> implements IMutator<C, T> {
	
	@Override
	public void set(C c, T t) throws IllegalAccessException {
		try {
			doSet(c, t);
		} catch (NullPointerException npe) {
			throw new NullPointerException("Cannot call " + getSetterDescription() + " on null instance");
		} catch (InvocationTargetException e) {
			Exceptions.throwAsRuntimeException(e.getCause());
		}
	}
	
	protected abstract void doSet(C c, T t) throws IllegalAccessException, InvocationTargetException;
	
	protected abstract String getSetterDescription();
	
}
