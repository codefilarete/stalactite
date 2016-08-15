package org.gama.reflection;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Member;
import java.lang.reflect.Method;
import java.util.Arrays;

import org.gama.lang.StringAppender;

/**
 * @author mary
 */
public class MutatorByMethod<C, T> extends AbstractMutator<C, T> implements MutatorByMember<C, T, Method> {
	
	private final Method setter;
	
	protected final Object[] methodParameters;
	
	public MutatorByMethod(Method setter) {
		super();
		this.setter = setter;
		this.setter.setAccessible(true);
		int parametersLength = this.setter.getParameterTypes().length;
		// method parameters instanciation to avoid extra array instanciation on each set(..) call 
		this.methodParameters = new Object[parametersLength];
	}
	
	@Override
	public Method getSetter() {
		return setter;
	}
	
	@Override
	protected void doSet(C c, T t) throws IllegalAccessException, InvocationTargetException {
		fixMethodParameters(t);
		getSetter().invoke(c, methodParameters);
	}
	
	protected void fixMethodParameters(T t) {
		this.methodParameters[0] = t;
	}
	
	@Override
	protected String getSetterDescription() {
		StringAppender description = new StringAppender(100);
		description.cat(getSetter().getDeclaringClass().getName(), ".", getSetter().getName(), "(");
		Class<?>[] parameterTypes = getSetter().getParameterTypes();
		if (parameterTypes.length > 0) {
			for (Class<?> paramType : parameterTypes) {
				description.cat(paramType.getSimpleName(), ", ");
			}
			description.cutTail(2).cat(")");
		}
		return description.toString();
	}
	
	@Override
	public AccessorByMember<C, T, ? extends Member> toAccessor() {
		Class<?> declaringClass = getSetter().getDeclaringClass();
		String propertyName = Accessors.propertyName(getSetter());
		AccessorByMethod<C, T> accessorByMethod = Accessors.accessorByMethod(declaringClass, propertyName);
		if (accessorByMethod == null) {
			return Accessors.accessorByField((Class<C>) declaringClass, propertyName);
		} else {
			return accessorByMethod;
		}
	}
	
	
	@Override
	public boolean equals(Object other) {
		// we base our implementation on the setter description because a setAccessible() call on the member changes its internal state
		// and I don't think it sould be taken into account for comparison
		return this == other ||
				(other instanceof MutatorByMethod
						&& getSetterDescription().equals(((MutatorByMethod) other).getSetterDescription())
						&& Arrays.equals(methodParameters, ((MutatorByMethod) other).methodParameters));
	}
	
	@Override
	public int hashCode() {
		return 31 * getSetter().hashCode() + Arrays.hashCode(methodParameters);
	}
}
