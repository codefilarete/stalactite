package org.gama.reflection;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Member;
import java.lang.reflect.Method;

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
		String propertyName = Accessors.getPropertyName(getSetter());
		AccessorByMethod<C, T> accessorByMethod = Accessors.accessorByMethod(declaringClass, propertyName);
		if (accessorByMethod == null) {
			return Accessors.accessorByField(declaringClass, propertyName);
		} else {
			return accessorByMethod;
		}
	}
}
