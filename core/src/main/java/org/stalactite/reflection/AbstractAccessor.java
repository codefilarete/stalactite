package org.stalactite.reflection;

import java.lang.reflect.InvocationTargetException;

/**
 * @author Guillaume Mary
 */
public abstract class AbstractAccessor<C, T> extends AbstractReflector<C, T> implements IAccessor<C, T> {
	
	@Override
	public T get(C c) {
		try {
			return doGet(c);
		} catch (Throwable t) {
			handleException(t, c);
			// shouldn't happen
			return null;
		}
	}
	
	protected abstract T doGet(C c) throws IllegalAccessException, InvocationTargetException;
	
	protected abstract String getGetterDescription();
}
