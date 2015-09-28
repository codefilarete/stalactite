package org.gama.reflection;

import java.lang.reflect.InvocationTargetException;
import java.util.List;

import org.gama.lang.Reflections;

/**
 * @author Guillaume Mary
 */
public class ListMutator<C extends List<T>, T> extends MutatorByMethod<C, T> {
	
	private int index;
	
	public ListMutator() {
		super(Reflections.findMethod(List.class, "set", Integer.TYPE, Object.class));
	}
	
	public ListMutator(int index) {
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
	protected void doSet(C c, T t) throws IllegalAccessException, InvocationTargetException {
		c.set(getIndex(), t);	// plus rapide que invoke
	}
	
	@Override
	public ListAccessor<C, T> toAccessor() {
		return new ListAccessor<>(getIndex());
	}
}
