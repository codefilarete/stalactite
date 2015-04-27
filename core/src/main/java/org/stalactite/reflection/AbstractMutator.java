package org.stalactite.reflection;

import java.lang.reflect.InvocationTargetException;

/**
 * @author Guillaume Mary
 */
public abstract class AbstractMutator<C, T> extends AbstractReflector<C, T> implements IMutator<C, T> {
	
	@Override
	public void set(C c, T t) {
		try {
			doSet(c, t);
		} catch (Throwable throwable) {
			handleException(throwable, c);
		}
	}
	
	protected abstract void doSet(C c, T t) throws IllegalAccessException, InvocationTargetException;
	
	protected abstract String getSetterDescription();
	
}
