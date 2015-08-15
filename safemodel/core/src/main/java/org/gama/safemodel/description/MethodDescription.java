package org.gama.safemodel.description;

import org.gama.lang.Reflections;

import java.lang.reflect.Method;

/**
 * @author Guillaume Mary
 */
public class MethodDescription<R> extends AbstracNamedMemberDescription<Method> {
	
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
}
