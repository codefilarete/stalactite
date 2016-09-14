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
import java.util.function.Supplier;
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
public class FluentMappingBuilder<T, I> implements IFluentMappingBuilder<T, I> {
	
	public static enum IdentifierPolicy {
		ALREADY_ASSIGNED,
		BEFORE_INSERT,
		AFTER_INSERT
	}
	
	/**
	 * Will start a {@link FluentMappingBuilder} for a given class which will target a table that as the class name.
	 * @param persistedClass the class to be persisted by the {@link ClassMappingStrategy} that will be created by {@link #build(Dialect)}
	 * @param identifierClass the class of the identifier
	 * @param <T> any type to be persisted
	 * @param <I> the type of the identifier
	 * @return a new {@link FluentMappingBuilder}
	 */
	public static <T, I> FluentMappingBuilder<T, I> from(Class<T> persistedClass, Class<I> identifierClass) {
		return from(persistedClass, identifierClass, new Table(persistedClass.getSimpleName()));
	}
	
	/**
	 * Will start a {@link FluentMappingBuilder} for a given class and a given target table.
	 * @param persistedClass the class to be persisted by the {@link ClassMappingStrategy} that will be created by {@link #build(Dialect)}
	 * @param identifierClass the class of the identifier
	 * @param table the table which will store instances of the persistedClass
	 * @param <T> any type to be persisted
	 * @param <I> the type of the identifier
	 * @return a new {@link FluentMappingBuilder}
	 */
	public static <T, I> FluentMappingBuilder<T, I> from(Class<T> persistedClass, Class<I> identifierClass, Table table) {
		return new FluentMappingBuilder<>(persistedClass, table);
	}
	
	private final Class<T> persistedClass;
	
	private final Table table;
	
	private List<Linkage> mapping;
	
	private IdentifierInsertionManager<T, I> identifierInsertionManager;
	
	private PropertyAccessor<T, I> identifierAccessor;
	
	private final LambdaMethodCapturer<T> spy;
	
	public FluentMappingBuilder(Class<T> persistedClass, Table table) {
		this.persistedClass = persistedClass;
		this.table = table;
		this.mapping = new ArrayList<>();
		
		// Code enhancer for creation of a proxy that will support functions invocations
		this.spy = new LambdaMethodCapturer<>(persistedClass);
	}
	
	public Table getTable() {
		return table;
	}
	
	private Method captureLambdaMethod(Function<T, ?> function) {
		return this.spy.capture(function);
	}
	
	private <O> Method captureLambdaMethod(BiConsumer<T, O> function) {
		return this.spy.capture(function);
	}
	
	@Override
	public <O> IFluentMappingBuilderColumnOptions<T, I> add(BiConsumer<T, O> function) {
		Method method = captureLambdaMethod(function);
		Class<?> columnType = Accessors.propertyType(method);
		String columnName = Accessors.propertyName(method);
		return add(method, columnName, columnType);
	}
	
	@Override
	public IFluentMappingBuilderColumnOptions<T, I> add(Function<T, ?> function) {
		Method method = captureLambdaMethod(function);
		Class<?> columnType = Accessors.propertyType(method);
		String columnName = Accessors.propertyName(method);
		return add(method, columnName, columnType);
	}
	
	@Override
	public IFluentMappingBuilderColumnOptions<T, I> add(Function<T, ?> function, String columnName) {
		Method method = captureLambdaMethod(function);
		Class<?> columnType = Accessors.propertyType(method);
		return add(method, columnName, columnType);
	}
	
	private IFluentMappingBuilderColumnOptions<T, I> add(Method method, String columnName, Class<?> columnType) {
		Column newColumn = table.new Column(columnName, columnType);
		PropertyAccessor<T, I> propertyAccessor = PropertyAccessor.of(method);
		this.mapping.stream().map(Linkage::getFunction).filter(propertyAccessor::equals)
				.findAny()
				.ifPresent(f -> { throw new IllegalArgumentException("Mapping is already defined by a method " + f.getAccessor()); });
		this.mapping.stream().map(Linkage::getColumn).filter(newColumn::equals)
				.findAny()
				.ifPresent(f -> { throw new IllegalArgumentException("Mapping is already defined for " + columnName); });
		this.mapping.add(new Linkage(propertyAccessor, newColumn));
		Class<IFluentMappingBuilderColumnOptions<T, I>> returnType = (Class) IFluentMappingBuilderColumnOptions.class;
		return new Overlay<>(ColumnOptions.class).overlay(this, returnType, new ColumnOptions() {
			@Override
			public FluentMappingBuilder identifier(IdentifierPolicy identifierPolicy) {
				if (FluentMappingBuilder.this.identifierAccessor != null) {
					throw new IllegalArgumentException("Identifier is already defined by " + identifierAccessor.getAccessor());
				}
				switch (identifierPolicy) {
					case ALREADY_ASSIGNED:
						Class<I> e = Accessors.onJavaBeanFieldWrapper(method,
								method::getReturnType,
								(Supplier<Class>) () -> method.getParameterTypes()[0],
								() -> boolean.class);
						FluentMappingBuilder.this.identifierInsertionManager = new AlreadyAssignedIdentifierManager<>(e);
						newColumn.primaryKey();
						break;
					default:
						throw new NotYetSupportedOperationException();
				}
				FluentMappingBuilder.this.identifierAccessor = propertyAccessor;
				return FluentMappingBuilder.this;	// not necessary because the overlay will return it for us, but cleaner (if we change our mind)
			}
		});
	}
	
	private Map<PropertyAccessor, Column> getMapping() {
		return mapping.stream().collect(HashMap::new, (hashMap, linkage) -> hashMap.put(linkage.getFunction(), linkage.getColumn()), (a, b) -> {});
	}
	
	private ClassMappingStrategy<T, I> buildClassMappingStrategy() {
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
	public ClassMappingStrategy<T, I> build(Dialect dialect) {
		// Assertion that binders are present: this will throw en exception if the binder is not found
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
