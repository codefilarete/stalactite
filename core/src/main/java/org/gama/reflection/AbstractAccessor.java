package org.gama.reflection;

import java.lang.reflect.InvocationTargetException;

/**
 * @author Guillaume Mary
 */
public abstract class AbstractAccessor<C, T> extends AbstractReflector<C> implements IAccessor<C, T> {
	
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
	
	@Override
	public boolean equals(Object obj) {
		return obj instanceof AbstractAccessor && getGetterDescription().equals(((AbstractAccessor) obj).getGetterDescription());
	}
	
	@Override
	public String toString() {
		return getGetterDescription();
	}
}
