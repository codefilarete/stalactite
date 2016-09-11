package org.gama.stalactite.persistence.engine;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.gama.lang.collection.ArrayIterator;
import org.gama.lang.collection.Iterables;
import org.gama.reflection.Accessors;
import org.gama.reflection.PropertyAccessor;
import org.gama.stalactite.persistence.id.manager.AlreadyAssignedIdentifierManager;
import org.gama.stalactite.persistence.id.manager.IdentifierInsertionManager;
import org.gama.stalactite.persistence.mapping.ClassMappingStrategy;
import org.gama.stalactite.persistence.sql.Dialect;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.persistence.structure.Table.Column;

/**
 * @author Guillaume Mary
 */
public class PersistenceMapper<T> implements IPersistenceMapper<T> {
	
	public static enum IdentifierPolicy {
		ALREADY_ASSIGNED,
		BEFORE_INSERT,
		AFTER_INSERT
	}
	
	public static <I> PersistenceMapper<I> with(Class<I> persistedClass, Table table) {
		return new PersistenceMapper<>(persistedClass, table);
	}
	
	private final Class<T> persistedClass;
	
	private final Table table;
	
	private List<Linkage> mapping;
	
	private IdentifierInsertionManager<T> identifierInsertionManager;
	
	private PropertyAccessor<T, ?> identifierAccessor;
	
	private final LambdaMethodCapturer<T> spy;
	
	public PersistenceMapper(Class<T> persistedClass, Table table) {
		this.persistedClass = persistedClass;
		this.table = table;
		this.mapping = new ArrayList<>();
		
		// Code enhancer for creation of a proxy that will support functions invocations
		this.spy = new LambdaMethodCapturer<>(persistedClass);
	}
	
	private Method captureLambdaMethod(Function<T, ?> function) {
		return this.spy.capture(function);
	}
	
	private <I> Method captureLambdaMethod(BiConsumer<T, I> function) {
		return this.spy.capture(function);
	}
	
	@Override
	public <I> IPersistenceMapperColumnOptions<T> add(BiConsumer<T, I> function) {
		Method method = captureLambdaMethod(function);
		Class<?> columnType = Accessors.propertyType(method);
		String columnName = Accessors.propertyName(method);
		return add(method, columnName, columnType);
	}
	
	@Override
	public IPersistenceMapperColumnOptions<T> add(Function<T, ?> function) {
		Method method = captureLambdaMethod(function);
		Class<?> columnType = Accessors.propertyType(method);
		String columnName = Accessors.propertyName(method);
		return add(method, columnName, columnType);
	}
	
	@Override
	public IPersistenceMapperColumnOptions<T> add(Function<T, ?> function, String columnName) {
		Method method = captureLambdaMethod(function);
		Class<?> columnType = Accessors.propertyType(method);
		return add(method, columnName, columnType);
	}
	
	private IPersistenceMapperColumnOptions<T> add(Method method, String columnName, Class<?> columnType) {
		Column newColumn = table.new Column(columnName, columnType);
		PropertyAccessor<T, Object> propertyAccessor = PropertyAccessor.of(method);
		this.mapping.add(new Linkage(propertyAccessor, newColumn));
		return new Overlay<>(ColumnOptions.class).overlay(this, (Class<IPersistenceMapperColumnOptions<T>>) (Class) IPersistenceMapperColumnOptions.class, new ColumnOptions() {
			@Override
			public PersistenceMapper identifier(IdentifierPolicy identifierPolicy) {
				if (PersistenceMapper.this.identifierAccessor != null) {
					throw new IllegalArgumentException("Identifier is already defined by " + identifierAccessor.getAccessor());
				}
				switch (identifierPolicy) {
					case ALREADY_ASSIGNED:
						PersistenceMapper.this.identifierInsertionManager = new AlreadyAssignedIdentifierManager<>();
						newColumn.primaryKey();
						break;
					default:
						throw new NotYetSupportedOperationException();
				}
				PersistenceMapper.this.identifierAccessor = propertyAccessor;
				return PersistenceMapper.this;	// not necessary because the overlay will return it for us, but cleaner (if we change our mind)
			}
		});
	}
	
	private Map<PropertyAccessor, Column> getMapping() {
		return mapping.stream().collect(HashMap::new, (hashMap, linkage) -> hashMap.put(linkage.getFunction(), linkage.getColumn()), (a, b) -> {});
	}
	
	private <I> ClassMappingStrategy<T, I> buildClassMappingStrategy() {
		Map<PropertyAccessor, Column> columnMapping = getMapping();
		List<Entry<PropertyAccessor, Column>> identifierProperties = columnMapping.entrySet().stream().filter(e -> e.getValue().isPrimaryKey()).collect
				(Collectors.toList());
		PropertyAccessor<T, I> identifierProperty;
		switch (identifierProperties.size()) {
			case 0:
				throw new IllegalArgumentException("Table without primary key is not supported");
			case 1:
				identifierProperty = (PropertyAccessor<T, I>) identifierProperties.get(0).getKey();
				break;
			default:
				throw new IllegalArgumentException("Multiple columned primary key is not supported");
		}
		
		return new ClassMappingStrategy<>(persistedClass, table, columnMapping, identifierProperty, this.identifierInsertionManager);
	}
	
	@Override
	public <I> ClassMappingStrategy<T, I> forDialect(Dialect dialect) {
		mapping.stream().map(Linkage::getColumn).forEach(c -> dialect.getColumnBinderRegistry().getBinder(c));
		return buildClassMappingStrategy();
	}
	
	
	private class Linkage {
		
		private final PropertyAccessor<T, ?> function;
		private final Column column;
		
		private Linkage(PropertyAccessor<T, ?> function, Column column) {
			this.function = function;
			this.column = column;
		}
		
		public PropertyAccessor<T, ?> getFunction() {
			return function;
		}
		
		public Column getColumn() {
			return column;
		}
	}
	
	private static class Overlay<V> {
		
		private final Class<V> clazz;
		
		Overlay(Class<V> clazz) {
			this.clazz = clazz;
		}
		
		public <T, U extends T> U overlay(T o, Class<U> interfazz, V surrogate) {
			InvocationHandler invocationRedirector = (proxy, method, args) -> {
				if (Iterables.copy(new ArrayIterator<>(clazz.getDeclaredMethods())).contains(method)) {
					invoke(surrogate, method, args);
					return o;
				} else {
					return invoke(o, method, args);
				}
			};
			// Which ClassLoader ?
			ClassLoader classLoader = Thread.currentThread().getContextClassLoader();//o.getClass().getClassLoader();
			return (U) Proxy.newProxyInstance(classLoader, new Class[] { interfazz }, invocationRedirector);
		}
		
		private Object invoke(Object target, Method method, Object[] args) throws Throwable {
			try {
				return method.invoke(target, args);
			} catch (InvocationTargetException e) {
				// we rethrow the main exception so caller will not be polluted by UndeclaredThrowableException + InvocationTargetException
				throw e.getCause();
			}
		}
	}
}
