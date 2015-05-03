package org.gama.lang.exception;

import java.lang.ref.SoftReference;
import java.util.NoSuchElementException;

import org.gama.lang.collection.ReadOnlyIterator;
import org.gama.lang.Reflections;
import org.gama.lang.collection.Iterables;
import org.gama.lang.collection.Iterables.Finder;
import org.stalactite.reflection.AccessorByField;

/**
 * 
 * @author Guillaume Mary 
 */
public abstract class Exceptions {
	
	/**
	 * Fait en sorte de toujours lever une RuntimeException même si t n'en est pas une.
	 * @param t 
	 * @throws t si c'est une RuntimeException, sinon une nouvelle RuntimeException avec <tt>t</tt> en tant que cause
	 */
	public static void throwAsRuntimeException(Throwable t) {
		if (t instanceof RuntimeException) {
			throw (RuntimeException) t;
		} else {
			throw new RuntimeException(t);
		}
	}
	
	private static volatile SoftReference<AccessorByField<Throwable, String>> MESSAGE_ACCESSOR;
	
	/**
	 * Renvoie un accesseur à l'attribut "detailMessage" de Throwable. Nécessaire parfois quand des classes d'Exception
	 * surcharge getMessage() et qu'il est impossible d'avoir le message unique de l'erreur.
	 * @return
	 */
	public static AccessorByField<Throwable, String> getDetailMessageGetter() {
		AccessorByField<Throwable, String> detailMessageAccessor = MESSAGE_ACCESSOR == null ? null : MESSAGE_ACCESSOR.get();
		if (detailMessageAccessor == null) {
			synchronized (Exceptions.class) {
				MESSAGE_ACCESSOR = new SoftReference<>(new AccessorByField<Throwable, String>(Reflections.getField(Throwable.class, "detailMessage")));
			}
		}
		return MESSAGE_ACCESSOR.get();
	}
	
	public static Throwable findExceptionInHierarchy(Throwable t, Class<Throwable> throwableClass) {
		return findExceptionInHierarchy(t, new ClassExceptionFilter(throwableClass));
	}
	
	public static Throwable findExceptionInHierarchy(Throwable t, Class<Throwable> throwableClass, String message) {
		return findExceptionInHierarchy(t, new ClassAndMessageExceptionFilter(throwableClass, message));
	}
	
	/**
	 * Recherche un Throwable dans la hiérarchie des causes de <i>t</i> en fonction
	 * de l'acceptation de <i>filter</i>
	 *
	 * @param t la source initiale d'erreur
	 * @param filter
	 * @return null si non trouvé
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
	 * Iterator de hiérarchie d'exception
	 */
	public static class ExceptionHierarchyIterator extends ReadOnlyIterator<Throwable> {
		
		private Throwable currentThrowable;
		
		public ExceptionHierarchyIterator(Throwable throwable) {
			this.currentThrowable = throwable;
		}
		
		@Override
		public boolean hasNext() {
			return currentThrowable.getCause() == null;
		}
		
		@Override
		public Throwable getNext() {
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
	
	public static class ClassExceptionFilter implements IExceptionFilter {
		
		private final Class<Throwable> targetClass;
		
		public ClassExceptionFilter(Class<Throwable> c) {
			this.targetClass = c;
		}
		
		public boolean accept(Throwable t) {
			return targetClass.isAssignableFrom(t.getClass());
		}
	}
	
	private static class ClassAndMessageExceptionFilter extends ClassExceptionFilter {
		
		private final String targetMessage;
		
		public ClassAndMessageExceptionFilter(Class<Throwable> c, String message) {
			super(c);
			this.targetMessage = message;
		}
		
		@Override
		public boolean accept(Throwable t) {
			return super.accept(t) && targetMessage.equalsIgnoreCase(getDetailMessageGetter().get(t));
		}
	}
	
}
