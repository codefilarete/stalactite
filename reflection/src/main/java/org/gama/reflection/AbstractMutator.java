package org.gama.reflection;

import java.lang.reflect.InvocationTargetException;

/**
 * @author Guillaume Mary
 */
public abstract class AbstractMutator<C, T> extends AbstractReflector<C> implements IMutator<C, T> {
	
	@Override
	public void set(C c, T t) {
		try {
			doSet(c, t);
		} catch (Throwable throwable) {
			handleException(throwable, c, t);
		}
	}
	
	protected abstract void doSet(C c, T t) throws IllegalAccessException, InvocationTargetException;
	
	protected abstract String getSetterDescription();
	
	/**
	 * Default implementation based on setter description
	 * @param obj the reference object with which to compare.
	 * @return true if this object has the same description as the other one, false otherwise
	 */
	@Override
	public boolean equals(Object obj) {
		return this == obj || (obj instanceof AbstractMutator && getSetterDescription().equals(((AbstractMutator) obj).getSetterDescription()));
	}
	
	@Override
	public int hashCode() {
		return getSetterDescription().hashCode();
	}
	
	@Override
	public String toString() {
		return getSetterDescription();
	}
}
