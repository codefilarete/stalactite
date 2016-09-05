package org.gama.reflection;

import java.lang.reflect.Array;
import java.lang.reflect.InvocationTargetException;

/**
 * @author Guillaume Mary
 */
// NB: I didn't manage to create AbstractMutator<C[], C> without having a "C cannot be cast to Object[]" from MetaModelAccessorBuilder
public class ArrayMutator<C> extends AbstractMutator<C, C> implements IReversibleMutator<C, C> {
	
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
	
	/**
	 * Same as {@link #set(Object, Object)} but using a direct access to the array
	 */
	public void set(C[] cs, C c) {
		cs[index] = c;
	}
	
	@Override
	public void set(C c, C other) {
		try {
			doSet(c, other);
		} catch (Throwable throwable) {
			handleException(throwable, c);
		}
	}
	
	@Override
	protected void doSet(C cs, C c) throws IllegalAccessException, InvocationTargetException {
		Array.set(cs, getIndex(), c);
	}
	
	@Override
	protected String getSetterDescription() {
		return "array mutator on index " + index;
	}
	
	@Override
	public ArrayAccessor<C> toAccessor() {
		return new ArrayAccessor<>(getIndex());
	}
}
