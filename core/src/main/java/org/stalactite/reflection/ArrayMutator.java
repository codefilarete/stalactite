package org.stalactite.reflection;

import java.lang.reflect.InvocationTargetException;

/**
 * @author Guillaume Mary
 */
public class ArrayMutator<C> extends AbstractMutator<C[], C> {
	
	private int index;
	
	public ArrayMutator() {
	}
	
	public ArrayMutator(int index) {
		this.index = index;
	}
	
	public int getIndex() {
		return index;
	}
	
	public void setIndex(int index) {
		this.index = index;
	}
	
	@Override
	protected void doSet(C[] cs, C c) throws IllegalAccessException, InvocationTargetException {
		cs[index] = c;
	}
	
	@Override
	protected String getSetterDescription() {
		return "array index";
	}
}
