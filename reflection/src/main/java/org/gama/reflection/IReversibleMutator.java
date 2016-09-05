package org.gama.reflection;

/**
 * @author Guillaume Mary
 */
public interface IReversibleMutator<C, T> extends IMutator<C, T> {
	
	IAccessor<C, T> toAccessor();
}
