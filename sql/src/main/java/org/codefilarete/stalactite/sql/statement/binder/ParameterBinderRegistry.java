package org.codefilarete.stalactite.sql.statement.binder;

import java.io.File;
import java.io.InputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.file.Path;
import java.sql.Blob;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.WeakHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.codefilarete.stalactite.sql.statement.SQLStatement.BindingException;
import org.codefilarete.tool.Duo;
import org.codefilarete.tool.Reflections;
import org.codefilarete.tool.bean.ClassIterator;
import org.codefilarete.tool.bean.InterfaceIterator;
import org.codefilarete.tool.collection.Arrays;
import org.codefilarete.tool.collection.Iterables;

import static java.util.stream.Collectors.toSet;

/**
 * Registry of {@link ParameterBinder}s according to their binding class.
 * <br>
 * Design note : this class can't implement {@link ParameterBinderIndex} because it already defines
 * {@link #getBinder(Class)} with generified Class type. This disallows to have a none generified version of it, which
 * is the default given while implementing {@link ParameterBinderIndex}&lt;Class&gt;. Moreover, it would disallow also
 * children classes to implement another type of key as index since classes can't implement twice an interface with 2
 * different generic types due to type erasure.
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
		
		public <E extends Enum<E>> ParameterBinder<E> newParameterBinder(Class<E> enumType) {
			return new NullAwareParameterBinder<>(factory.apply(enumType));
		}
	}
	
	private final WeakHashMap<Class, ParameterBinder> binderPerType = new WeakHashMap<>();
	
	public ParameterBinderRegistry() {
		registerParameterBinders();
	}
	
	public Map<Class, ParameterBinder> getBinderPerType() {
		return binderPerType;
	}
	
	public <T> void register(Class<T> clazz, ParameterBinder<T> parameterBinder) {
		binderPerType.put(clazz, parameterBinder);
	}
	
	protected void registerParameterBinders() {
		// Note that enum types are registered dynamically
		register(String.class, DefaultParameterBinders.STRING_BINDER);
		register(Double.class, DefaultParameterBinders.DOUBLE_BINDER);
		register(Double.TYPE, DefaultParameterBinders.DOUBLE_PRIMITIVE_BINDER);
		register(Number.class, DefaultParameterBinders.NUMBER_BINDER);
		register(Float.class, DefaultParameterBinders.FLOAT_BINDER);
		register(Float.TYPE, DefaultParameterBinders.FLOAT_PRIMITIVE_BINDER);
		register(BigDecimal.class, DefaultParameterBinders.BIGDECIMAL_BINDER);
		register(BigInteger.class, new NullAwareParameterBinder<>(new LambdaParameterBinder<>(DefaultParameterBinders.LONG_PRIMITIVE_BINDER, BigInteger::valueOf, BigInteger::longValue)));
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
		register(LocalTime.class, DefaultParameterBinders.LOCALTIME_BINDER);
		register(Instant.class, new LambdaParameterBinder<>(DefaultParameterBinders.LONG_BINDER, Instant::ofEpochMilli, Instant::toEpochMilli));
		register(java.sql.Timestamp.class, DefaultParameterBinders.TIMESTAMP_BINDER);
		register(Boolean.class, DefaultParameterBinders.BOOLEAN_BINDER);
		register(Boolean.TYPE, DefaultParameterBinders.BOOLEAN_PRIMITIVE_BINDER);
		register(InputStream.class, DefaultParameterBinders.BINARYSTREAM_BINDER);
		register(Blob.class, DefaultParameterBinders.BLOB_BINDER);
		register(ZoneId.class, DefaultParameterBinders.ZONEID_BINDER);
		register(UUID.class, DefaultParameterBinders.UUID_BINDER);
		register(Path.class, DefaultParameterBinders.PATH_BINDER);
		register(File.class, DefaultParameterBinders.FILE_BINDER);
	}
	
	/**
	 * Gives the registered {@link ParameterBinder} for the given type.
	 * 
	 * @param clazz a class
	 * @return the registered {@link ParameterBinder} for the given type
	 * @throws BindingException if the binder doesn't exist
	 */
	public <T> ParameterBinder<T> getBinder(Class<T> clazz) {
		ParameterBinder<T> parameterBinder = binderPerType.get(clazz);
		if (parameterBinder == null) {
			parameterBinder = lookupForBinder(clazz);
		}
		return parameterBinder;
	}
	
	private <T> Set<Duo<Class, ParameterBinder>> lookupForCompatibleBinder(Class<T> clazz) {
		if (clazz.isEnum()) {
			return Arrays.asSet(new Duo(clazz, binderPerType.computeIfAbsent(clazz, k -> new NullAwareParameterBinder<>(new OrdinalEnumParameterBinder(k)))));
		}
		
		Iterator<Class> classHierarchy = Iterables.concat(Iterables.ofElements(clazz), new ClassIterator(clazz));
		return Iterables.stream(classHierarchy)
				.map(classAncestor -> {
					ParameterBinder parameterBinder = binderPerType.get(classAncestor);
					return parameterBinder == null ? null : (Set<Duo<Class, ParameterBinder>>) Arrays.asSet(new Duo<>((Class) clazz, parameterBinder));
				})
				// we keep only class for which a binder exists
				.filter(Objects::nonNull)
				.findFirst()
				.orElseGet(() -> {
					// if no binder was found during class hierarchy scan, we scan the interfaces
					return Iterables.stream(new InterfaceIterator(clazz))
							.map(pawn -> new Duo<>(pawn, binderPerType.get(pawn)))
							// we keep only class for which a binder exists
							.filter(duo -> duo.getRight() != null)
							.collect(Collectors.toSet());
				});
	}
	
	private <T> ParameterBinder<T> lookupForBinder(Class<T> clazz) {
		Set<Duo<Class, ParameterBinder>> binderPerInterface = lookupForCompatibleBinder(clazz);
		if (binderPerInterface.size() > 1) {
			throw new BindingException("Multiple binders found for " + Reflections.toString(clazz)
					+ ", please register one for any of : " + Iterables.stream(binderPerInterface).map(Duo::getLeft).map(Reflections::toString).collect(toSet()));
		} else if (binderPerInterface.isEmpty()) {
			throw new BindingException("No binder found for type " + Reflections.toString(clazz));
		}
		ParameterBinder<T> foundBinder = Iterables.first(binderPerInterface).getRight();
		// we put the found binder to save computation of a next call (optional action)
		binderPerType.put(clazz, foundBinder);
		return foundBinder;
	}
}