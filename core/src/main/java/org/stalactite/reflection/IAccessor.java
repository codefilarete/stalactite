package org.stalactite.reflection;

/**
 * @author Guillaume Mary
 */
public interface IAccessor<C, T> {
	T get(C c);
	
	IMutator<C, T> toMutator();
}
