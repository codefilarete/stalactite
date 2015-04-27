package org.stalactite.reflection;

import java.lang.reflect.Array;
import java.lang.reflect.InvocationTargetException;

/**
 * @author Guillaume Mary
 */
// NB: pas réussi à faire AbstractAccessor<C[], C> sans que ça génère un "C cannot be cast to Object[]" par MetaModelAccessorBuilder
public class ArrayAccessor<C> extends AbstractAccessor<C, C> {
	
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
	
	/**
	 * Equivalent pour get(C) mais en accès direct par un tableau de C
	 */
	public C get(C[] c) {
		return get((C) c);
	}
	
	@Override
	public C get(C c) {
		try {
			return doGet(c);
		} catch (Throwable t) {
			handleException(t, c);
			// shouldn't happen
			return null;
		}
	}
	
	@Override
	protected C doGet(C cs) throws IllegalAccessException, InvocationTargetException {
		return (C) Array.get(cs, getIndex());
	}
	
	@Override
	protected String getGetterDescription() {
		return "array index";
	}
}
