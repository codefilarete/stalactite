package org.gama.reflection;

import java.lang.reflect.Array;
import java.lang.reflect.InvocationTargetException;

/**
 * @author Guillaume Mary
 */
// NB: pas réussi à faire AbstractMutator<C[], C> sans que ça génère un "C cannot be cast to Object[]" par MetaModelAccessorBuilder
public class ArrayMutator<C> extends AbstractMutator<C, C> {
	
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
	 * Equivalent pour set(C, C) mais en accès direct par un tableau de C
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
		return "array index mutator";
	}
	
	@Override
	public ArrayAccessor<C> toAccessor() {
		return new ArrayAccessor<>(getIndex());
	}
}
