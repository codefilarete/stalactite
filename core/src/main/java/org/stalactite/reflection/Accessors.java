package org.stalactite.reflection;

import java.lang.reflect.Field;
import java.lang.reflect.Method;

import org.gama.lang.Reflections;
import org.gama.lang.Strings;

/**
 * @author Guillaume Mary
 */
public final class Accessors {
	
	public static <C, T> AccessorByMethod<C, T> accessorByMethod(Field field) {
		return accessorByMethod(field.getDeclaringClass(), field.getName());
	}
	
	public static <C, T> AccessorByMethod<C, T> accessorByMethod(Class clazz, String propertyName) {
		String capitalizedProperty = Strings.capitalize(propertyName);
		Method getter = Reflections.getMethod(clazz, "get" + capitalizedProperty);
		if (getter == null) {
			// try for boolean
			Field field = Reflections.getField(clazz, propertyName);
			if (field != null && Boolean.class.isAssignableFrom(field.getType())) {
				getter = Reflections.getMethod(clazz, "is" + capitalizedProperty);
			} // nothing found : neither get nor is => return null
		}
		return getter == null ? null : new AccessorByMethod<C, T>(getter);
	}
	
	public static <C, T> AccessorByField<C, T> accessorByField(Field field) {
		return new AccessorByField<>(field);
	}
	
	public static <C, T> AccessorByField<C, T> accessorByField(Class clazz, String propertyName) {
		Field propertyField = Reflections.getField(clazz, propertyName);
		return accessorByField(propertyField);
	}
	
	public static <C, T> MutatorByMethod<C, T> mutatorByMethod(Field field) {
		return mutatorByMethod(field.getDeclaringClass(), field.getName());
	}
	
	public static <C, T> MutatorByMethod<C, T> mutatorByMethod(Class clazz, String propertyName) {
		Field propertyField = Reflections.getField(clazz, propertyName);
		String capitalizedProperty = Strings.capitalize(propertyName);
		Method setter = Reflections.getMethod(clazz, "set" + capitalizedProperty, propertyField.getType());
		return setter == null ? null : new MutatorByMethod<C, T>(setter);
	}
	
	public static <C, T> MutatorByField<C, T> mutatorByField(Field field) {
		return new MutatorByField<>(field);
	}
	
	public static <C, T> MutatorByField<C, T> mutatorByField(Class clazz, String propertyName) {
		Field propertyField = Reflections.getField(clazz, propertyName);
		return mutatorByField(propertyField);
	}
	
	public static Field wrappedField(AccessorByMethod accessorByMethod) {
		Method getter = accessorByMethod.getGetter();
		return wrappedField(getter);
	}
	
	public static Field wrappedField(Method fieldWrapper) {
		String propertyName = getPropertyName(fieldWrapper);
		return Reflections.getField(fieldWrapper.getDeclaringClass(), propertyName);
	}
	
	public static String getPropertyName(Method fieldWrapper) {
		String propertyName = fieldWrapper.getName();
		if (Boolean.class.isAssignableFrom(fieldWrapper.getReturnType()) && propertyName.startsWith("is")) {
			propertyName = propertyName.substring(2);
		} else if (fieldWrapper.getName().startsWith("get") || fieldWrapper.getName().startsWith("set")) {
			propertyName = propertyName.substring(3);
		} else {
			throw new IllegalArgumentException("Field wrapper " + fieldWrapper.getDeclaringClass().getName()+"."+fieldWrapper.getName() + " doesn't feet encapsulation naming convention");
		}
		propertyName = Strings.uncapitalize(propertyName);
		return propertyName;
	}
	
	private Accessors() {
	}
}
