package org.gama.reflection;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;

/**
 * @author mary
 */
public class MutatorByField<C, T> extends AbstractMutator<C, T> implements MutatorByMember<C, T, Field> {
	
	private final Field field;
	
	public MutatorByField(Field field) {
		super();
		this.field = field;
		this.field.setAccessible(true);
	}
	
	@Override
	public Field getSetter() {
		return field;
	}
	
	@Override
	protected void doSet(C c, T t) throws IllegalAccessException, InvocationTargetException {
		getSetter().set(c, t);
	}
	
	@Override
	protected String getSetterDescription() {
		return "mutator for field " + getSetter().toString();
	}
	
	@Override
	public AccessorByField<C, T> toAccessor() {
		return new AccessorByField<>(getSetter());
	}
	
	@Override
	public boolean equals(Object other) {
		// we base our implementation on the setter description because a setAccessible() call on the member changes its internal state
		// and I don't think it sould be taken into account for comparison
		return this == other
				|| (other instanceof MutatorByField && getSetterDescription().equals(((MutatorByField) other).getSetterDescription()));
	}
	
	@Override
	public int hashCode() {
		return getSetter().hashCode();
	}
}
