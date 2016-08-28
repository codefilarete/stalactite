package org.gama.sql.binder;

import java.io.InputStream;
import java.util.Date;
import java.util.WeakHashMap;

/**
 * Registry of {@link ParameterBinder}s according to their binding class.
 *
 * @author Guillaume Mary
 */
public class ParameterBinderRegistry {
	
	private final WeakHashMap<Class, ParameterBinder> parameterBinders = new WeakHashMap<>();
	
	public ParameterBinderRegistry() {
		registerParameterBinders();
	}
	
	public WeakHashMap<Class, ParameterBinder> getParameterBinders() {
		return parameterBinders;
	}
	
	public <T> void register(Class<T> clazz, ParameterBinder<T> parameterBinder) {
		parameterBinders.put(clazz, parameterBinder);
	}
	
	protected void registerParameterBinders() {
		register(String.class, DefaultParameterBinders.STRING_BINDER);
		register(Double.class, DefaultParameterBinders.DOUBLE_BINDER);
		register(Double.TYPE, DefaultParameterBinders.DOUBLE_PRIMITIVE_BINDER);
		register(Float.class, DefaultParameterBinders.FLOAT_BINDER);
		register(Float.TYPE, DefaultParameterBinders.FLOAT_PRIMITIVE_BINDER);
		register(Long.class, DefaultParameterBinders.LONG_BINDER);
		register(Long.TYPE, DefaultParameterBinders.LONG_PRIMITIVE_BINDER);
		register(Integer.class, DefaultParameterBinders.INTEGER_BINDER);
		register(Integer.TYPE, DefaultParameterBinders.INTEGER_PRIMITIVE_BINDER);
		register(Date.class, new DateBinder());
		register(Boolean.class, DefaultParameterBinders.BOOLEAN_BINDER);
		register(Boolean.TYPE, DefaultParameterBinders.BOOLEAN_PRIMITIVE_BINDER);
		register(InputStream.class, DefaultParameterBinders.BINARYSTREAM_BINDER);
	}
	
	/**
	 * Gives the registered {@link ParameterBinder} for the given type.
	 * 
	 * @param clazz a class
	 * @return the registered {@link ParameterBinder} for the given type
	 * @throws UnsupportedOperationException if the binder doesn't exist
	 */
	public <T> ParameterBinder<T> getBinder(Class<T> clazz) throws UnsupportedOperationException {
		ParameterBinder<T> parameterBinder = getParameterBinders().get(clazz);
		if (parameterBinder == null) {
			throw newMissingBinderException(clazz);
		}
		return parameterBinder;
	}
	
	private UnsupportedOperationException newMissingBinderException(Class clazz) {
		return new UnsupportedOperationException("No parameter binder found for type " + clazz.getName());
	}
	
}
