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
	
	/**
	 * Default implementation based on getter description
	 * @param obj the reference object with which to compare.
	 * @return true if this object has the same description as the other one, false otherwise
	 */
	@Override
	public boolean equals(Object obj) {
		return this == obj || (obj instanceof AbstractAccessor && getGetterDescription().equals(((AbstractAccessor) obj).getGetterDescription()));
	}
	
	@Override
	public int hashCode() {
		return getGetterDescription().hashCode();
	}
	
	@Override
	public String toString() {
		return getGetterDescription();
	}
}
