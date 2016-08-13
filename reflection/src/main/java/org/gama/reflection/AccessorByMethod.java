package org.gama.reflection;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Member;
import java.lang.reflect.Method;
import java.util.Arrays;

/**
 * @author mary
 */
public class AccessorByMethod<C, T> extends AbstractAccessor<C, T> implements AccessorByMember<C, T, Method> {
	
	private final Method getter;
	
	private final Object[] methodParameters;
	
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
	
	@Override
	public MutatorByMember<C, T, ? extends Member> toMutator() {
		Class<?> declaringClass = getGetter().getDeclaringClass();
		String propertyName = Accessors.propertyName(getGetter());
		MutatorByMethod<C, T> mutatorByMethod = Accessors.mutatorByMethod((Class<C>) declaringClass, propertyName);
		if (mutatorByMethod == null) {
			return Accessors.mutatorByField(declaringClass, propertyName);
		} else {
			return mutatorByMethod;
		}
	}
	
	@Override
	public boolean equals(Object other) {
		return this == other || 
				(other instanceof AccessorByMethod
						&& getGetter().equals(((AccessorByMethod) other).getGetter())
						&& Arrays.equals(methodParameters, ((AccessorByMethod) other).methodParameters));
	}
	
	@Override
	public int hashCode() {
		return 31 * getGetter().hashCode() + Arrays.hashCode(methodParameters);
	}
}
