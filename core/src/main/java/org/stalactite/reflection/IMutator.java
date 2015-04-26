package org.stalactite.reflection;

/**
 * @author Guillaume Mary
 */
public interface IMutator<C, T> {
	void set(C c, T t) throws IllegalAccessException;
}
