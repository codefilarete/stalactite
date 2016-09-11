package org.gama.reflection;

/**
 * @author Guillaume Mary
 */
public interface IReversibleAccessor<C, T> extends IAccessor<C, T> {
	
	IMutator<C, T> toMutator();
	
}
