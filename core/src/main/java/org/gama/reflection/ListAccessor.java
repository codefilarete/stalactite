package org.gama.reflection;

import java.lang.reflect.InvocationTargetException;
import java.util.List;

import org.gama.lang.Reflections;

/**
 * @author Guillaume Mary
 */
public class ListAccessor<C extends List<T>, T> extends AccessorByMethod<C, T> {
	
	private int index;
	
	public ListAccessor() {
		super(Reflections.getMethod(List.class, "get", Integer.TYPE));
	}
	
	public ListAccessor(int index) {
		this();
		setIndex(index);
	}
	
	public void setIndex(int index) {
		this.index = index;
	}
	
	public int getIndex() {
		return index;
	}
	
	@Override
	protected T doGet(C c, Object ... args) throws IllegalAccessException, InvocationTargetException {
		return c.get(getIndex());
	}
	
	@Override
	protected String getGetterDescription() {
		return "java.util.List.get(int)";
	}
	
	@Override
	public ListMutator<C, T> toMutator() {
		return new ListMutator<>(getIndex());
	}
}
