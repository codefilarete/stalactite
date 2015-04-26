package org.stalactite.reflection;

import java.lang.reflect.InvocationTargetException;
import java.util.List;

import org.stalactite.lang.Reflections;

/**
 * @author Guillaume Mary
 */
public class AccessorForList<C extends List<T>, T> extends AccessorByMethod<C, T> {
	
	private int index;
	
	public AccessorForList() {
		super(Reflections.getMethod(List.class, "get", Integer.TYPE));
	}
	
	public void setIndex(int index) {
		this.index = index;
	}
	
	public int getIndex() {
		return index;
	}
	
	@Override
	protected T doGet(C c) throws IllegalAccessException, InvocationTargetException {
		return c.get(getIndex());
	}
}
