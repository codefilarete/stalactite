package org.stalactite.lang;

import java.lang.reflect.AccessibleObject;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;

import javax.annotation.Nonnull;

import org.stalactite.lang.collection.ReadOnlyIterator;

/**
 * @author mary
 */
public final class Reflections {
	
	public static void ensureAccessible(AccessibleObject accessibleObject) {
		accessibleObject.setAccessible(true);
	}
	
	public static <T> Constructor<T> getDefaultConstructor(Class<T> clazz) {
		try {
			return clazz.getConstructor();
		} catch (NoSuchMethodException e) {
			throw new UnsupportedOperationException("Class " + clazz.getName() + " doesn't have a default constructor");
		}
	}
	
	/**
	 * Parcoureur de la hiérarchie d'une classe
	 */
	public static class ClassIterator extends ReadOnlyIterator<Class> {
		
		private Class currentClass, topBoundAncestor;
		
		public ClassIterator(@Nonnull Class currentClass) {
			this(currentClass, Object.class);
		}
		
		public ClassIterator(@Nonnull Class currentClass, @Nonnull Class topBoundAncestor) {
			this.currentClass = currentClass;
			this.topBoundAncestor = topBoundAncestor;
		}
		
		@Override
		public boolean hasNext() {
			return !currentClass.equals(topBoundAncestor);
		}
		
		@Override
		protected Class getNext() {
			Class next = currentClass;
			currentClass = currentClass.getSuperclass();
			return next;
		}
	}
	
	public static class FieldIterator extends ReadOnlyIterator<Field> {
		
		private ClassIterator classIterator;
		private ArrayIterator<Field> fieldIterator;
		
		public FieldIterator(@Nonnull Class currentClass) {
			this(new ClassIterator(currentClass));
		}
		
		public FieldIterator(ClassIterator classIterator) {
			this.classIterator = classIterator;
			this.fieldIterator = new ArrayIterator<>(classIterator.next().getDeclaredFields());
		}
		
		@Override
		public boolean hasNext() {
			return fieldIterator.hasNext() || this.classIterator.hasNext();
		}
		
		@Override
		protected Field getNext() {
			if (!fieldIterator.hasNext()) {
				Field[] declaredFields = classIterator.next().getDeclaredFields();
				fieldIterator = new ArrayIterator<>(declaredFields);
			}
			return fieldIterator.next();
		}
		
		private class ArrayIterator<O> extends ReadOnlyIterator<O> {
			
			private O[] array;
			private int currentIndex = 0, maxIndex;
			
			public ArrayIterator(O[] array) {
				this.array = array;
				this.maxIndex = array.length;
			}
			
			@Override
			public boolean hasNext() {
				return currentIndex < maxIndex;
			}
			
			@Override
			protected O getNext() {
				return this.array[currentIndex++];
			}
		}
		
	}
}
