package org.stalactite.reflection;

import java.lang.reflect.InvocationTargetException;
import java.util.List;

import org.stalactite.lang.Reflections;

/**
 * @author Guillaume Mary
 */
public class AccessorForList<C, T> extends AccessorByMethod<C, T> {
	
	private int index;
	
	public AccessorForList() {
		super(Reflections.getMethod(List.class, "get", Integer.TYPE), Reflections.getMethod(List.class, "set", Integer.TYPE, Object.class));
	}
	
	public void setIndex(int index) {
		this.index = index;
	}
	
	public int getIndex() {
		return index;
	}
	
	@Override
	protected T doGet(C c) throws IllegalAccessException, InvocationTargetException {
		return (T) getGetter().invoke(c, index);
	}
	
	@Override
	protected void doSet(C c, T t) throws IllegalAccessException, InvocationTargetException {
		getSetter().invoke(c, index, t);
	}
}
