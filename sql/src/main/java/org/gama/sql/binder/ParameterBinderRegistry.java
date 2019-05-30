package org.gama.sql.binder;

import java.io.InputStream;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Blob;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Date;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.function.Function;

import org.gama.lang.Reflections;
import org.gama.lang.bean.InterfaceIterator;
import org.gama.lang.collection.Iterables;
import org.gama.sql.dml.SQLStatement.BindingException;

import static java.util.stream.Collectors.toSet;
import static org.gama.lang.collection.Iterables.first;

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
	
	public enum EnumBindType {
		
		NAME(NameEnumParameterBinder::new),
		ORDINAL(OrdinalEnumParameterBinder::new);
		
		private final Function<Class<? extends Enum>, AbstractEnumParameterBinder> factory;
		
		EnumBindType(Function<Class<? extends Enum>, AbstractEnumParameterBinder> factory) {
			this.factory = factory;
		}
		
		public <E extends Enum<E>> AbstractEnumParameterBinder<E> newParameterBinder(Class<E> enumType) {
			return factory.apply(enumType);
		}
	}
	
	private final WeakHashMap<Class, ParameterBinder> bindersPerType = new WeakHashMap<>();
	
	public ParameterBinderRegistry() {
		registerParameterBinders();
	}
	
	public Map<Class, ParameterBinder> getBindersPerType() {
		return bindersPerType;
	}
	
	public <T> void register(Class<T> clazz, ParameterBinder<T> parameterBinder) {
		bindersPerType.put(clazz, parameterBinder);
	}
	
	protected void registerParameterBinders() {
		register(String.class, DefaultParameterBinders.STRING_BINDER);
		register(Double.class, DefaultParameterBinders.DOUBLE_BINDER);
		register(Double.TYPE, DefaultParameterBinders.DOUBLE_PRIMITIVE_BINDER);
		register(Float.class, DefaultParameterBinders.FLOAT_BINDER);
		register(Float.TYPE, DefaultParameterBinders.FLOAT_PRIMITIVE_BINDER);
		register(BigDecimal.class, DefaultParameterBinders.BIGDECIMAL_BINDER);
		register(Long.class, DefaultParameterBinders.LONG_BINDER);
		register(Long.TYPE, DefaultParameterBinders.LONG_PRIMITIVE_BINDER);
		register(Integer.class, DefaultParameterBinders.INTEGER_BINDER);
		register(Integer.TYPE, DefaultParameterBinders.INTEGER_PRIMITIVE_BINDER);
		register(Byte.class, DefaultParameterBinders.BYTE_BINDER);
		register(Byte.TYPE, DefaultParameterBinders.BYTE_PRIMITIVE_BINDER);
		register(byte[].class, DefaultParameterBinders.BYTES_BINDER);
		register(Date.class, DefaultParameterBinders.DATE_BINDER);
		register(java.sql.Date.class, DefaultParameterBinders.DATE_SQL_BINDER);
		register(LocalDate.class, DefaultParameterBinders.LOCALDATE_BINDER);
		register(LocalDateTime.class, DefaultParameterBinders.LOCALDATETIME_BINDER);
		register(Boolean.class, DefaultParameterBinders.BOOLEAN_BINDER);
		register(Boolean.TYPE, DefaultParameterBinders.BOOLEAN_PRIMITIVE_BINDER);
		register(InputStream.class, DefaultParameterBinders.BINARYSTREAM_BINDER);
		register(Blob.class, DefaultParameterBinders.BLOB_BINDER);
		register(URL.class, DefaultParameterBinders.URL_BINDER);
		register(ZoneId.class, DefaultParameterBinders.ZONEID_BINDER);
	}
	
	/**
	 * Gives the registered {@link ParameterBinder} for the given type.
	 * 
	 * @param clazz a class
	 * @return the registered {@link ParameterBinder} for the given type
	 * @throws BindingException if the binder doesn't exist
	 */
	public <T> ParameterBinder<T> getBinder(Class<T> clazz) {
		ParameterBinder<T> parameterBinder = getBindersPerType().get(clazz);
		if (parameterBinder == null) {
			parameterBinder = lookupForBinder(clazz);
		}
		return parameterBinder;
	}
	
	private <T> ParameterBinder<T> lookupForBinder(Class<T> clazz) {
		Set<Class> assignableType = new HashSet<>();
		InterfaceIterator interfaceIterator = new InterfaceIterator(clazz);
		Iterables.copy(interfaceIterator, assignableType);
		Set<Entry<Class, ParameterBinder>> compatibleBinders =
				bindersPerType.entrySet().stream().filter(e -> assignableType.contains(e.getKey())).collect(toSet());
		if (compatibleBinders.size() == 1) {
			ParameterBinder foundBinder = first(compatibleBinders).getValue();
			// we put the found binder to save computation of a next call (optional action)
			bindersPerType.put(clazz, foundBinder);
			return foundBinder;
		} else if (compatibleBinders.size() > 1) {
			throw new BindingException("Multiple binders found for " + Reflections.toString(clazz)
					+ ", please register a dedicated one : " + compatibleBinders.stream().map(Entry::getKey).map(Reflections::toString).collect(toSet()));
		} else {
			throw newMissingBinderException(clazz);
		}
	}
	
	private BindingException newMissingBinderException(Class clazz) {
		return new BindingException("No binder found for type " + Reflections.toString(clazz));
	}
	
}
