package org.stalactite.reflection;

import java.lang.reflect.InvocationTargetException;

/**
 * @author Guillaume Mary
 */
public class ArrayAccessor<C> extends AbstractAccessor<C[], C> {
	
	private int index;
	
	public ArrayAccessor() {
	}
	
	public ArrayAccessor(int index) {
		this.index = index;
	}
	
	public int getIndex() {
		return index;
	}
	
	public void setIndex(int index) {
		this.index = index;
	}
	
	@Override
	protected C doGet(C[] cs) throws IllegalAccessException, InvocationTargetException {
		return cs[index];
	}
	
	@Override
	protected String getGetterDescription() {
		return "array index";
	}
}
