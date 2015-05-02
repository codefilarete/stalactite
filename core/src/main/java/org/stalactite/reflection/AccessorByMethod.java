package org.stalactite.reflection;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * @author mary
 */
public class AccessorByMethod<C, T> extends AbstractAccessor<C, T> implements AccessorByMember<Method> {
	
	private final Method getter;
	
	protected final Object[] methodParameters;
	
	public AccessorByMethod(Method getter) {
		this.getter = getter;
		this.getter.setAccessible(true);
		int parametersLength = this.getter.getParameterTypes().length;
		// method parameters instanciation to avoid extra array instanciation on each get(..) call
		this.methodParameters = new Object[parametersLength];
	}
	
	@Override
	public Method getGetter() {
		return getter;
	}
	
	@Override
	public T get(C c) {
		fixMethodParameters();
		return get(c, methodParameters);
	}
	
	public AccessorByMethod<C, T> setParameters(Object ... values) {
		for (int i = 0; i < values.length; i++) {
			setParameter(i, values[i]);
		}
		return this;
	}
	
	public AccessorByMethod<C, T> setParameter(int index, Object value) {
		this.methodParameters[index] = value;
		return this;
	}
	
	/**
	 * To override for complex arguments building
	 */
	protected void fixMethodParameters() {
		
	}
	
	public T get(C c, Object ... params) {
		try {
			return doGet(c, params);
		} catch (Throwable t) {
			handleException(t, c, params);
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
