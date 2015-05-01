package org.stalactite.reflection;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * @author mary
 */
public class AccessorByMethod<C, T> extends AbstractAccessor<C, T> implements AccessorByMember<Method> {
	
	private final Method getter;
	
	public AccessorByMethod(Method getter) {
		this.getter = getter;
		this.getter.setAccessible(true);
	}
	
	@Override
	public Method getGetter() {
		return getter;
	}
	
	@Override
	public T get(C c) {
		return get(c, new Object[]{});
	}
	
	public T get(C c, Object ... args) {
		try {
			return doGet(c, args);
		} catch (Throwable t) {
			handleException(t, c, args);
			// shouldn't happen
			return null;
		}
	}
	
	@Override
	// NB: set final to force override doGet(C, Object ...) and so to avoid mistake
	protected final T doGet(C c) throws IllegalAccessException, InvocationTargetException {
		return doGet(c, new Object[] {});
	}
	
	protected T doGet(C c, Object ... args) throws IllegalAccessException, InvocationTargetException {
		return (T) getGetter().invoke(c, args);
	}
	
	@Override
	protected String getGetterDescription() {
		return getGetter().getDeclaringClass().getName() + "." + getGetter().getName() + "()";
	}
}
