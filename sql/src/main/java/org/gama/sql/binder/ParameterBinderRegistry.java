package org.gama.sql.binder;

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
		register(String.class, new StringBinder());
		register(Double.class, new DoubleBinder());
		register(Double.TYPE, new DoubleBinder());
		register(Float.class, new FloatBinder());
		register(Float.TYPE, new FloatBinder());
		register(Long.class, new LongBinder());
		register(Long.TYPE, new LongBinder());
		register(Integer.class, new IntegerBinder());
		register(Integer.TYPE, new IntegerBinder());
		register(Date.class, new DateBinder());
		register(Boolean.class, new BooleanBinder());
		register(Boolean.TYPE, new BooleanBinder());
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
