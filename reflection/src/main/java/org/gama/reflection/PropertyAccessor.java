package org.gama.reflection;

import java.lang.reflect.Field;

import org.gama.lang.Reflections;

/**
 * @author Guillaume Mary
 */
public class PropertyAccessor<C, T> implements IAccessor<C, T>, IMutator<C, T> {
	
	public static <C, T> PropertyAccessor<C, T> forProperty(Field field) {
		return forProperty((Class<C>) field.getDeclaringClass(), field.getName());
	}
	
	public static <C, T> PropertyAccessor<C, T> forProperty(Class<C> clazz, String propertyName) {
		IAccessor<C, T> propertyGetter = Accessors.accessorByMethod(clazz, propertyName);
		if (propertyGetter == null) {
			propertyGetter = new AccessorByField<>(Reflections.findField(clazz, propertyName));
		}
		IMutator<C, T> propertySetter = Accessors.mutatorByMethod(clazz, propertyName);
		if (propertySetter == null) {
			propertySetter = new MutatorByField<>(Reflections.findField(clazz, propertyName));
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
	public T get(C c) {
		return this.accessor.get(c);
	}
	
	public void set(C c, T t) {
		this.mutator.set(c, t);
	}
	
	@Override
	public IAccessor<C, T> toAccessor() {
		return getAccessor();
	}
	
	@Override
	public IMutator<C, T> toMutator() {
		return getMutator();
	}
	
	
}
