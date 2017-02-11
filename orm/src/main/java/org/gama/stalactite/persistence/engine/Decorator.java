package org.gama.stalactite.persistence.engine;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.gama.lang.collection.Arrays;

/**
 * A class that is aimed to redirect some methods to an instance whereas other methods remain invoked on a main instance.
 * 
 * @param <C> the type that will intercept methods call, interface expected
 * 
 * @author Guillaume Mary
 */
public class Decorator<C> {
	
	/**
	 * Shortcut for a new {@link Decorator} instance on which {@link #decorate(Object, Class, Object)} is called.
	 * If several usages of same intercepting interface is done, consider using a shared instance of a {@link Decorator} for it.
	 * 
	 * @param toBeDecorated the support for default methods
	 * @param interfazz the type to be returned (only interface supported, must extends T and V)
	 * @param extensionSurrogate the instance that will intercept method calls of the V interface on the returned proxy
	 * @param <C> the extension type
	 * @param <T> the type to be decorated
	 * @param <X> an interface that must extends T and C (multiple inheritance, couldn't manage to express it in the signature because compiler warns
	 * 				about V is not an interface)
	 * @return a JDK proxy that implements U
	 */
	public static <C, T, X extends T> X extend(T toBeDecorated, Class<X> interfazz, C extensionSurrogate) {
		return new Decorator<>((Class<C>) extensionSurrogate.getClass()).decorate(toBeDecorated, interfazz, extensionSurrogate);
	}
	
	private final Set<Method> methodsToBeIntercepted;
	
	Decorator(Class<C> clazz) {
		if (!clazz.isInterface()) {
			throw new UnsupportedOperationException("Only interfaces are supported as argument"
					+ " because only multiple inheritance is supported by interfaces");
		}
		methodsToBeIntercepted = Collections.unmodifiableSet(new HashSet<>(Arrays.asSet(clazz.getMethods())));
	}
	
	/**
	 * 
	 * @param toBeDecorated the support for default methods
	 * @param interfazz the type to be returned (only interface supported, must extends T and V)
	 * @param extensionSurrogate the instance that will intercept method calls of the V interface on the returned proxy
	 * @param <T> the type to be decorated
	 * @param <X> an interface that must extends T and C (multiple inheritance, couldn't manage to express it in the signature because compiler warns
	 * 				about V is not an interface)
	 * @return a JDK proxy that implements U
	 */
	public <T, X extends T> X decorate(T toBeDecorated, Class<X> interfazz, C extensionSurrogate) {
		InvocationHandler invocationRedirector = (proxy, method, args) -> {
			if (methodsToBeIntercepted.contains(method)) {
				return invoke(extensionSurrogate, method, args);
			} else {
				return invoke(toBeDecorated, method, args);
			}
		};
		// Which ClassLoader ? Thread, target ?
		// we use the target's one because Thread can live forever so it might lead to memory leak
		ClassLoader classLoader = toBeDecorated.getClass().getClassLoader();
		return (X) Proxy.newProxyInstance(classLoader, new Class[] { interfazz }, invocationRedirector);
	}
	
	private Object invoke(Object target, Method method, Object[] args) throws Throwable {
		try {
			return method.invoke(target, args);
		} catch (InvocationTargetException e) {
			// we rethrow the main exception so caller will not be polluted by UndeclaredThrowableException + InvocationTargetException
			throw e.getCause();
		}
	}
}
