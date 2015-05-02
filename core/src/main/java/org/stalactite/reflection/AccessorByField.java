package org.stalactite.reflection;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;

/**
 * @author mary
 */
public class AccessorByField<C, T> extends AbstractAccessor<C, T> implements AccessorByMember<C, T, Field> {
	
	private final Field field;
	
	public AccessorByField(Field field) {
		this.field = field;
		this.field.setAccessible(true);
	}
	
	@Override
	public Field getGetter() {
		return field;
	}
	
	@Override
	protected T doGet(C c) throws IllegalAccessException, InvocationTargetException {
		return (T) getGetter().get(c);
	}
	
	@Override
	public String getGetterDescription() {
		return "accessor for field " + getGetter().toString();
	}
	
	@Override
	public MutatorByField<C, T> toMutator() {
		return new MutatorByField<>(getGetter());
	}
}
