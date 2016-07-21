package org.gama.lang;

import java.lang.reflect.AccessibleObject;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

import org.gama.lang.bean.FieldIterator;
import org.gama.lang.bean.MethodIterator;
import org.gama.lang.collection.Iterables;
import org.gama.lang.collection.Iterables.Finder;
import org.gama.lang.collection.Iterables.Mapper;
import org.gama.lang.exception.Exceptions;

/**
 * @author Guillaume Mary
 */
public final class Reflections {
	
	public static void ensureAccessible(AccessibleObject accessibleObject) {
		accessibleObject.setAccessible(true);
	}
	
	public static <T> Constructor<T> getDefaultConstructor(Class<T> clazz) {
		try {
			return clazz.getDeclaredConstructor();
		} catch (NoSuchMethodException e) {
			throw new UnsupportedOperationException("Class " + clazz.getName() + " doesn't have a default constructor");
		}
	}
	
	public static Map<String, Field> mapFieldsOnName(Class clazz) {
		Mapper<String, Field> fieldVisitor = new Mapper<String, Field>(new LinkedHashMap<String, Field>()) {
			@Override
			protected String getKey(Field field) {
				return field.getName();
			}
		};
		return Iterables.filter(new FieldIterator(clazz), fieldVisitor);
	}
	
	/**
	 * Returns the field with the given signature elements. Class hierarchy is checked also until Object class. 
	 *
	 * @param clazz the class of the field
	 * @param name the name of the field
	 * @return the found field, null possible
	 */
	public static Field findField(Class clazz, final String name) {
		Finder<Field> fieldVisitor = new Finder<Field>() {
			@Override
			public boolean accept(Field field) {
				return field.getName().equals(name);
			}
		};
		return Iterables.filter(new FieldIterator(clazz), fieldVisitor);
	}
	
	/**
	 * Same as {@link #findField(Class, String)} but throws a {@link org.gama.lang.Reflections.MemberNotFoundException}
	 * if the field is not found.
	 *
	 * @param clazz the class of the method
	 * @param name the name of the method
	 * @return the found method, never null
	 */
	public static Field getField(Class clazz, final String name) {
		Field field = findField(clazz, name);
		if (field == null) {
			throw new MemberNotFoundException("Field " + name + " on " + clazz.getName() + " was not found");
		}
		return field;
	}
	
	/**
	 * Returns the method with the given signature elements. Class hierarchy is checked also until Object class. 
	 * 
	 * @param clazz the class of the method
	 * @param name the name of the method
	 * @param argTypes the argument types of the method
	 * @return the found method, null possible
	 */
	public static Method findMethod(Class clazz, final String name, final Class... argTypes) {
		Finder<Method> methodVisitor = new Finder<Method>() {
			@Override
			public boolean accept(Method method) {
				return method.getName().equals(name) && Arrays.equals(method.getParameterTypes(), argTypes);
			}
		};
		return Iterables.filter(new MethodIterator(clazz), methodVisitor);
	}
	
	/**
	 * Same as {@link #findMethod(Class, String, Class[])} but throws a {@link org.gama.lang.Reflections.MemberNotFoundException}
	 * if the method is not found.
	 * 
	 * @param clazz the class of the method
	 * @param name the name of the method
	 * @param argTypes the argument types of the method
	 * @return the found method, never null
	 */
	public static Method getMethod(Class clazz, final String name, final Class... argTypes) {
		Method method = findMethod(clazz, name, argTypes);
		if (method == null) {
			throw new MemberNotFoundException("Method " + name + "(" + new StringAppender().ccat(argTypes, ", ").toString() + ") on " + clazz.getName() + " was not found");
		}
		return method;
	}
	
	public static <E> E newInstance(Class<E> clazz) {
		try {
			return getDefaultConstructor(clazz).newInstance();
		} catch (Throwable t) {
			throw Exceptions.asRuntimeException(t);
		}
	}
	
	
	public static class MemberNotFoundException extends RuntimeException {
		public MemberNotFoundException(String message) {
			super(message);
		}
	}
}
