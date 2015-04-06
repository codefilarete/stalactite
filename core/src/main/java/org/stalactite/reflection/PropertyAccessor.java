package org.stalactite.reflection;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Member;

/**
 * @author mary
 */
public abstract class PropertyAccessor<C, T, M extends Member> {
	
	private final M accessor;
	
	public PropertyAccessor(M accessor) {
		this.accessor = accessor;
	}
	
	public M getAccessor() {
		return accessor;
	}
	
	public abstract T get(C c) throws IllegalAccessException, InvocationTargetException;
	
	public abstract void set(C c, T t) throws IllegalAccessException, InvocationTargetException;
}
