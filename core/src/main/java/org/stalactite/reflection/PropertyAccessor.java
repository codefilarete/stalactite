package org.stalactite.reflection;

import java.lang.reflect.Field;

import org.stalactite.lang.Reflections;

/**
 * @author Guillaume Mary
 */
public class PropertyAccessor<C, T> implements IAccessor<C, T>, IMutator<C, T> {
	
	public static <C, T> PropertyAccessor<C, T> forProperty(Field field) {
		return forProperty((Class<C>) field.getDeclaringClass(), field.getName());
	}
	
	public static <C, T> PropertyAccessor<C, T> forProperty(Class<C> clazz, String propertyName) {
		IAccessor<C, T> propertyGetter = AccessorByMethod.forProperty(clazz, propertyName);
		if (propertyGetter == null) {
			propertyGetter = new AccessorByField<>(Reflections.getField(clazz, propertyName));
		}
		IMutator<C, T> propertySetter = MutatorByMethod.forProperty(clazz, propertyName);
		if (propertySetter == null) {
			propertySetter = new FieldMutator<>(Reflections.getField(clazz, propertyName));
		}
		return new PropertyAccessor<>(propertyGetter, propertySetter);
	}
	
	
	private final IAccessor<C, T> accessor;
	private final IMutator<C, T> mutator;
	
	public PropertyAccessor(IAccessor<C, T> accessor, IMutator<C, T> mutator) {
		this.accessor = accessor;
		this.mutator = mutator;
	}
	
	public IAccessor<C, T> getAccessor() {
		return accessor;
	}
	
	public IMutator<C, T> getMutator() {
		return mutator;
	}
	
	@Override
	public T get(C c) throws IllegalAccessException {
		return this.accessor.get(c);
	}
	
	public void set(C c, T t) throws IllegalAccessException {
		this.mutator.set(c, t);
	}
}
