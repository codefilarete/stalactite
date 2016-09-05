package org.gama.reflection;

/**
 * @author Guillaume Mary
 */
@FunctionalInterface
public interface IAccessor<C, T> {
	
	T get(C c);
}
