package org.gama.reflection;

import java.lang.reflect.Method;

/**
 * @author Guillaume Mary
 */
public class PropertyAccessorByMethod<C, T> extends PropertyAccessor<C, T> {
	
	public PropertyAccessorByMethod(Method getter, Method setter) {
		super(new AccessorByMethod<C, T>(getter), new MutatorByMethod<C, T>(setter));
	}
}
