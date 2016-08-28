package org.gama.reflection;

import java.lang.reflect.InvocationTargetException;
import java.util.List;

import org.gama.lang.Reflections;

/**
 * Dedicated class to {@link List#get(int)} accessor
 * 
 * @author Guillaume Mary
 */
public class ListAccessor<C extends List<T>, T> extends AccessorByMethod<C, T> {
	
	/* Implementation note:
	 * The index of the get() method is mapped to the first argument of super attribute "methodParameters" through setParameter(0, index)
	 * and getParameter(0). We could have used a dedicated attribute "index" but it requires implementation of equald/hashcode. Whereas reuse
	 * of "methodParameters" allow comparison (equals()) with another AccessorByMethod that is not a ListAccessor.
	 */
	
	/**
	 * Default constructor without index. Will lead to error if {@link #setIndex(int)} is not called.
	 */
	public ListAccessor() {
		super(Reflections.findMethod(List.class, "get", Integer.TYPE));
	}
	
	public ListAccessor(int index) {
		this();
		setIndex(index);
	}
	
	public void setIndex(int index) {
		// we reuse the super parameter method
		setParameter(0, index);
	}
	
	public int getIndex() {
		// preventing NullPointerException
		Object parameter = getParameter(0);
		return parameter == null ? 0 : (int) parameter;
	}
	
	@Override
	protected T doGet(C c, Object ... args) throws IllegalAccessException, InvocationTargetException {
		return c.get(getIndex());
	}
	
	@Override
	protected String getGetterDescription() {
		return "java.util.List.get(" + getIndex() +")";
	}
	
	@Override
	public ListMutator<C, T> toMutator() {
		return new ListMutator<>(getIndex());
	}
}
