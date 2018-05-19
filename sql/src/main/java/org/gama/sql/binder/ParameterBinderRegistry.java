package org.gama.sql.binder;

import java.io.InputStream;
import java.net.URL;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Date;
import java.util.Map;
import java.util.WeakHashMap;

import org.gama.lang.Reflections;

/**
 * Registry of {@link ParameterBinder}s according to their binding class.
 * <br>
 * NB: this class can't implement {@link ParameterBinderIndex} because it already defines {@link #getBinder(Class)} with generified Class type.
 * This disallows to have a none generified version of it, which is the default given while implementing {@link ParameterBinderIndex}&lt;Class&gt;
 * Moreover it would disallow also children classes to implement another type of key as index since classes can't implement twice an interface
 * with 2 differents generic type due to type erasure.
 *
 * @author Guillaume Mary
 */
public class ParameterBinderRegistry {
	
	private final WeakHashMap<Class, ParameterBinder> parameterBinders = new WeakHashMap<>();
	
	public ParameterBinderRegistry() {
		registerParameterBinders();
	}
	
	public Map<Class, ParameterBinder> getParameterBinders() {
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
		register(Date.class, DefaultParameterBinders.DATE_BINDER);
		register(java.sql.Date.class, DefaultParameterBinders.DATE_SQL_BINDER);
		register(LocalDate.class, DefaultParameterBinders.LOCALDATE_BINDER);
		register(LocalDateTime.class, DefaultParameterBinders.LOCALDATETIME_BINDER);
		register(Boolean.class, DefaultParameterBinders.BOOLEAN_BINDER);
		register(Boolean.TYPE, DefaultParameterBinders.BOOLEAN_PRIMITIVE_BINDER);
		register(InputStream.class, DefaultParameterBinders.BINARYSTREAM_BINDER);
		register(URL.class, DefaultParameterBinders.URL_BINDER);
		register(ZoneId.class, DefaultParameterBinders.ZONEID_BINDER);
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
		return new UnsupportedOperationException("No parameter binder found for type " + Reflections.toString(clazz));
	}
	
}
