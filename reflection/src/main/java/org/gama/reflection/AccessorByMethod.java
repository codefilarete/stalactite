package org.gama.reflection;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Member;
import java.lang.reflect.Method;
import java.util.Arrays;

import org.gama.lang.StringAppender;

/**
 * @author Guillaume Mary
 */
public class AccessorByMethod<C, T> extends AbstractAccessor<C, T> implements AccessorByMember<C, T, Method>, IReversibleAccessor<C, T> {
	
	private final Method getter;
	
	private final Object[] methodParameters;
	
	public AccessorByMethod(Method getter) {
		this(getter, new Object[getter.getParameterTypes().length]);
	}
	
	public AccessorByMethod(Method getter, Object ... arguments) {
		this.getter = getter;
		this.getter.setAccessible(true);
		this.methodParameters = arguments;
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
	
	public Object getParameter(int index) {
		return methodParameters[index];
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
		StringAppender arguments = new StringAppender(Arrays.deepToString(methodParameters));
		// removing '[' and ']'
		arguments.cutHead(1).cutTail(1);
		return getGetter().getDeclaringClass().getName() + "." + getGetter().getName() + "(" + arguments + ")";
	}
	
	@Override
	public MutatorByMember<C, T, ? extends Member> toMutator() {
		Class<?> declaringClass = getGetter().getDeclaringClass();
		String propertyName = Accessors.propertyName(getGetter());
		MutatorByMethod<C, T> mutatorByMethod = Accessors.mutatorByMethod((Class<C>) declaringClass, propertyName, getGetter().getReturnType());
		if (mutatorByMethod == null) {
			try {
				return Accessors.mutatorByField(declaringClass, propertyName);
			} catch (NoSuchFieldException e) {
				throw new NotReversibleAccessor("Can't find a mutator for " + getGetter());
			}
		} else {
			return mutatorByMethod;
		}
	}
	
	@Override
	public boolean equals(Object other) {
		// We base our implementation on the getter String because a setAccessible() call on the member changes its internal state
		// and I don't think it sould be taken into account for comparison
		// We could base it on getGetterDescription() but it requires more computation
		return this == other || 
				(other instanceof AccessorByMethod
						&& getGetter().toString().equals(((AccessorByMethod) other).getGetter().toString())
						&& Arrays.equals(methodParameters, ((AccessorByMethod) other).methodParameters));
	}
	
	@Override
	public int hashCode() {
		return 31 * getGetter().hashCode() + Arrays.hashCode(methodParameters);
	}
}
