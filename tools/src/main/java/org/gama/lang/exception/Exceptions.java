package org.gama.lang.exception;

import java.lang.ref.SoftReference;
import java.lang.reflect.Field;
import java.util.NoSuchElementException;

import org.gama.lang.Reflections;
import org.gama.lang.collection.Iterables;
import org.gama.lang.collection.Iterables.Finder;
import org.gama.lang.collection.ReadOnlyIterator;

/**
 * 
 * @author Guillaume Mary 
 */
public abstract class Exceptions {
	
	/**
	 * Convert a {@link Throwable} into a {@link RuntimeException}. Do nothing if the {@link Throwable} is already a {@link RuntimeException},
	 * else instanciate a {@link RuntimeException} with the exception as the init cause.
	 * <b>Please use with caution because doing this can be considered as a bad practice.</b>
	 *
	 * @param t any kinf of exception
	 * @return the {@link Throwable} itself if it's already a {@link RuntimeException},
	 * 			else a {@link RuntimeException} whose cause is the {@link Throwable} argument
	 */
	public static RuntimeException asRuntimeException(Throwable t) {
		if (t instanceof RuntimeException) {
			return  (RuntimeException) t;
		} else {
			return new RuntimeException(t);
		}
	}
	
	private static volatile SoftReference<Field> MESSAGE_ACCESSOR;
	
	/**
	 * Return the {@link Throwable#detailMessage} accessor. It's sometimes necessary to have it when Exception's subclasses override
	 * {@link Throwable#getMessage()} which then disallow access to the original error message.
	 * @return the {@link Field} that represents {@link Throwable#getMessage()}
	 */
	public static Field getDetailMessageGetter() {
		Field detailMessageAccessor = MESSAGE_ACCESSOR == null ? null : MESSAGE_ACCESSOR.get();
		if (detailMessageAccessor == null) {
			synchronized (Exceptions.class) {
				MESSAGE_ACCESSOR = new SoftReference<>(Reflections.findField(Throwable.class, "detailMessage"));
				MESSAGE_ACCESSOR.get().setAccessible(true);
			}
		}
		return MESSAGE_ACCESSOR.get();
	}
	
	public static <T> T findExceptionInHierarchy(Throwable t, Class<T> throwableClass) {
		return (T) findExceptionInHierarchy(t, new ClassExceptionFilter<>(throwableClass));
	}
	
	public static <T> T findExceptionInHierarchy(Throwable t, Class<T> throwableClass, String message) {
		return (T) findExceptionInHierarchy(t, new ClassAndMessageExceptionFilter<>(throwableClass, message));
	}
	
	/**
	 * Look up a {@link Throwable} in the causes hierarchy of the {@link Throwable} argument according to a {@link IExceptionFilter} 
	 *
	 * @param t the initial stack error
	 * @param filter a filter
	 * @return null if not found
	 */
	public static Throwable findExceptionInHierarchy(Throwable t, final IExceptionFilter filter) {
		Finder<Throwable> finder = new Finder<Throwable>() {
			@Override
			public boolean accept(Throwable t) {
				return filter.accept(t);
			}
		};
		return Iterables.filter(new ExceptionHierarchyIterator(t), finder);
	}
	
	/**
	 * Iterator on {@link Throwable} causes
	 */
	public static class ExceptionHierarchyIterator extends ReadOnlyIterator<Throwable> {
		
		private Throwable currentThrowable;
		
		public ExceptionHierarchyIterator(Throwable throwable) {
			this.currentThrowable = throwable;
		}
		
		@Override
		public boolean hasNext() {
			return currentThrowable.getCause() != null;
		}
		
		@Override
		public Throwable next() {
			try {
				currentThrowable = currentThrowable.getCause();
			} catch (NullPointerException e) {
				throw new NoSuchElementException();
			}
			return currentThrowable;
		}
	}
	
	public interface IExceptionFilter {
		boolean accept(Throwable t);
	}
	
	public static class ClassExceptionFilter<T> implements IExceptionFilter {
		
		private final Class<T> targetClass;
		
		public ClassExceptionFilter(Class<T> c) {
			this.targetClass = c;
		}
		
		public boolean accept(Throwable t) {
			return targetClass.isAssignableFrom(t.getClass());
		}
	}
	
	private static class ClassAndMessageExceptionFilter<T> extends ClassExceptionFilter<T> {
		
		private final String targetMessage;
		
		public ClassAndMessageExceptionFilter(Class<T> c, String message) {
			super(c);
			this.targetMessage = message;
		}
		
		@Override
		public boolean accept(Throwable t) {
			String throwableMessage;
			try {
				throwableMessage = (String) getDetailMessageGetter().get(t);
			} catch (IllegalAccessException e) {
				throw Exceptions.asRuntimeException(e);
			}
			return super.accept(t) && targetMessage.equalsIgnoreCase(throwableMessage);
		}
	}
	
}
