package org.gama.reflection;

import java.lang.reflect.Field;

/**
 * @author Guillaume Mary
 */
public class PropertyAccessorByField<C, T> extends PropertyAccessor<C, T> {
	
	public PropertyAccessorByField(Field field) {
		super(new AccessorByField<C, T>(field), new MutatorByField<C, T>(field));
	}
	
}
