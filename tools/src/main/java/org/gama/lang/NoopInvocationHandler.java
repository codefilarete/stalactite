package org.gama.lang;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

import org.gama.lang.collection.Iterables;

/**
 * InvocationHandler that does nothing. Usefull to create no-operation proxy (for mocking services) or intercept a
 * paricular method.
 * 
 * @see #mock(Class) 
 * @author Guillaume Mary
 */
public class NoopInvocationHandler implements InvocationHandler {

	/**
	 * Ease the creation of an interface stub
	 *
	 * @param interfazz an interface
	 * @param <T> type interface
	 * @return a no-operation proxy, of type T
	 */
	public static <T> T mock(Class<T> interfazz) {
		return (T) Proxy.newProxyInstance(NoopInvocationHandler.class.getClassLoader(), new Class[] {interfazz}, new NoopInvocationHandler());
	}

	/**
	 * Implemented to do nothing, always return null, excepte for equals() and hashCode() methods
	 * @param proxy
	 * @param method
	 * @param args
	 * @return null
	 * @throws Throwable
	 */
	@Override
	public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
		Object toReturn = null;
		if (isEqualsMethod(method)) {
			// Only consider equal when proxies are identical.
			toReturn = proxy == args[0];
		} else if (isHashCodeMethod(method)) {
			// Use hashCode of reference proxy.
			toReturn = System.identityHashCode(proxy);
		}
		return toReturn;
	}
	
	/**
	 * Determine whether the given method is an "equals" method.
	 */
	public static boolean isEqualsMethod(Method method) {
		return method.getName().equals("equals") && Iterables.first(method.getParameterTypes()) == Object.class && method.getReturnType() == boolean.class;
	}
	
	/**
	 * Determine whether the given method is a "hashCode" method.
	 */
	public static boolean isHashCodeMethod(Method method) {
		return method.getName().equals("hashCode") && method.getParameterTypes().length == 0 && method.getReturnType() == int.class;
	}
}
