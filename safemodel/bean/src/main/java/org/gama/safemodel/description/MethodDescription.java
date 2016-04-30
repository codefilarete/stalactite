package org.gama.safemodel.description;

import java.lang.reflect.Method;

import org.gama.lang.Reflections;

/**
 * @author Guillaume Mary
 */
public class MethodDescription<R> extends AbstracNamedMemberDescription<Method> {
	
	/**
	 * Build a {@link MethodDescription} from the description of a method: name, owning class and type 
	 *
	 * @throws IllegalArgumentException if the given method return type is not the real one
	 * @throws Reflections.MemberNotFoundException if the method is not found in the hierarchy of the owning class
	 * @see Reflections#getMethod(Class, String, Class[]) 
	 */
	public static <R> MethodDescription<R> method(Class declaringClass, String name, Class<R> returnType, Class... parameterTypes) {
		MethodDescription<R> methodDescription = new MethodDescription<>(declaringClass, name, parameterTypes);
		if (!methodDescription.getReturnType().equals(returnType)) {
			throw new IllegalArgumentException("Wrong return type given: declared "+returnType.getName() + " but is " + methodDescription.getReturnType().getName());
		}
		return methodDescription;
	}
	
	public final Method method;
	
	public MethodDescription(Class declaringClass, String name, Class... parameterTypes) {
		this(Reflections.getMethod(declaringClass, name, parameterTypes));
	}
	
	public MethodDescription(Method method) {
		super(method);
		this.method = method;
	}
	
	public Class[] getParameterTypes() {
		return method.getParameterTypes();
	}
	
	public Class<R> getReturnType() {
		return (Class<R>) method.getReturnType();
	}
	
	public Method getMethod() {
		return method;
	}
}
