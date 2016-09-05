package org.gama.reflection;

/**
 * @author Guillaume Mary
 */
@FunctionalInterface
public interface IMutator<C, T> {
	
	void set(C c, T t);
}
