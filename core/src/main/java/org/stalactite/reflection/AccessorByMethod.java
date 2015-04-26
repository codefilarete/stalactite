package org.stalactite.reflection;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import org.stalactite.lang.Reflections;
import org.stalactite.lang.Strings;

/**
 * @author mary
 */
public class AccessorByMethod<C, T> extends AbstractAccessor<C, T> implements AccessorByMember<Method> {
	
	private final Method getter;
	
	public static AccessorByMethod forProperty(Class clazz, String propertyName) {
		String capitalizedProperty = Strings.capitalize(propertyName);
		Method getter = Reflections.getMethod(clazz, "get" + capitalizedProperty);
		return getter == null ? null : new AccessorByMethod(getter);
	}
	
	public AccessorByMethod(Method getter) {
		this.getter = getter;
		this.getter.setAccessible(true);
	}
	
	@Override
	public Method getGetter() {
		return getter;
	}
	
	@Override
	protected T doGet(C c) throws IllegalAccessException, InvocationTargetException {
		return (T) getGetter().invoke(c);
	}
	
	@Override
	protected String getGetterDescription() {
		return getGetter().getDeclaringClass().getName() + "." + getGetter().getName() + "()";
	}
}
