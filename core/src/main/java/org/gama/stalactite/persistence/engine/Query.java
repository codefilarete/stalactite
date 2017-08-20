package org.gama.stalactite.persistence.engine;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.danekja.java.util.function.serializable.SerializableBiConsumer;
import org.danekja.java.util.function.serializable.SerializableFunction;
import org.gama.lang.function.LazyInitializer;
import org.gama.reflection.MethodReferenceCapturer;
import org.gama.sql.ConnectionProvider;
import org.gama.sql.binder.ParameterBinder;
import org.gama.sql.binder.ParameterBinderProvider;
import org.gama.sql.dml.ReadOperation;
import org.gama.sql.dml.StringParamedSQL;
import org.gama.sql.result.ResultSetConverter;

import static org.gama.sql.binder.NullAwareParameterBinder.ALWAYS_SET_NULL_INSTANCE;

/**
 * A class aimed at querying the database and creating Java beans from it.
 * 
 * @author Guillaume Mary
 */
public class Query<T> {
	
	/** Type of bean that will be built by {@link #execute(ConnectionProvider)} */
	private final Class<T> rootBeanType;
	
	/** The sql that contains the select clause */
	private final CharSequence sql;
	
	/** The definition of root bean instanciation */
	private BeanCreationDefinition<T, ?> beanCreationDefinition;
	
	/** Mappings between selected columns and bean property setter */
	private final List<ColumnMapping> columnMappings = new ArrayList<>();
	
	/** The "registry" of {@link ParameterBinder}s, for column reading as well as sql argument setting */
	private final ParameterBinderProvider<Class> parameterBinderProvider;
	
	/** {@link ParameterBinder}s per ({@link java.sql.PreparedStatement}) parameter */
	private final Map<String, ParameterBinder> sqlParameterBinders = new HashMap<>();
	
	/** SQL argument values (for where clause, or anywhere else) */
	private final Map<String, Object> sqlArguments = new HashMap<>();
	
	public Query(Class<T> rootBeanType, CharSequence sql, ParameterBinderProvider<Class> parameterBinderProvider) {
		this.rootBeanType = rootBeanType;
		this.sql = sql;
		this.parameterBinderProvider = parameterBinderProvider;
	}
	
	/**
	 * Defines the key column and the way to create the bean
	 * 
	 * @param columnName the key column name
	 * @param columnType the type of the column, which is also that of the factory argument
	 * @param factory the factory function that will instanciate new beans
	 * @param <I> the type of the column, which is also that of the factory argument
	 * @return this
	 */
	public <I> Query<T> mapKey(String columnName, Class<I> columnType, SerializableFunction<I, T> factory) {
		this.beanCreationDefinition = new BeanCreationDefinition<>(columnName, columnType, factory);
		return this;
	}
	
	/**
	 * Defines a mapping between a column of the query and a bean property through its setter
	 *
	 * @param columnName a column name
	 * @param columnType the type of the column, which is also that of the setter argument
	 * @param setter the setter function
	 * @param <I> the type of the column, which is also that of the setter argument
	 * @return this
	 */
	public <I> Query<T> map(String columnName, Class<I> columnType, SerializableBiConsumer<T, I> setter) {
		add(new ColumnMapping<>(columnName, columnType, setter));
		return this;
	}
	
	/**
	 * Defines a mapping between a column of the query and a bean property through its setter.
	 * WARNING : Column type will be deduced from the setter type. To do it, some bytecode enhancement is required, therefore it's not the cleanest
	 * way to define the binding. Prefer {@link #map(String, Class, SerializableBiConsumer)}
	 *
	 * @param columnName a column name
	 * @param setter the setter function
	 * @param <I> the type of the column, which is also that of the setter argument
	 * @return this
	 */
	public <I> Query<T> map(String columnName, SerializableBiConsumer<T, I> setter) {
		map(columnName, null, setter);
		return this;
	}
	
	private <I> void add(ColumnMapping<T, I> columnWire) {
		this.columnMappings.add(columnWire);
	}
	
	/**
	 * Executes the query onto the connection given by the {@link ConnectionProvider}. Transforms the result to a list of beans thanks to the
	 * definition given through {@link #mapKey(String, Class, SerializableFunction)}, {@link #map(String, Class, SerializableBiConsumer)}
	 * and {@link #map(String, SerializableBiConsumer)} methods.
	 * 
	 * @param connectionProvider the object that will given the {@link java.sql.Connection}
	 * @return a {@link List} filled by the instances built
	 */
	public List<T> execute(ConnectionProvider connectionProvider) {
		if (beanCreationDefinition == null) {
			throw new IllegalArgumentException("Bean creation is not defined, use mapKey(..)");
		}
		
		// we avoid useless need of a default constructor on target class due to MethodReferenceCapturer by making it lazy intialized
		LazyInitializer<MethodReferenceCapturer> lazyMethodCapturerProvider = new LazyInitializer<MethodReferenceCapturer>() {
			@Override
			protected MethodReferenceCapturer createInstance() {
				return new MethodReferenceCapturer();
			}
		};
		Class<?> columnType = beanCreationDefinition.getColumn().getValueType();
		if (columnType == null) {
			// column type wasn't defined (not encouraged) => we're going to try to find it by capture the method (factory) argument type
			Method setter = lazyMethodCapturerProvider.get().findMethod(beanCreationDefinition.factory);
			columnType = setter.getParameterTypes()[0];
		}
		ParameterBinder idParameterBinder = this.parameterBinderProvider.getBinder(columnType);
		ResultSetConverter<?, T> transformer = new ResultSetConverter<>(rootBeanType, beanCreationDefinition.getColumn().getName(), idParameterBinder, beanCreationDefinition.getFactory());
		for (ColumnMapping<T, Object> columnMapping : columnMappings) {
			columnType = columnMapping.getColumn().getValueType();
			if (columnType == null) {
				// column type wasn't defined (bad practice) => we're going to try to find it by capture the method (factory) argument type
				Method setter = lazyMethodCapturerProvider.get().findMethod(columnMapping.getSetter());
				columnType = setter.getParameterTypes()[0];
			}
			transformer.add(columnMapping.getColumn().getName(), parameterBinderProvider.getBinder(columnType), columnMapping.getSetter());
		}
		ReadOperation<String> readOperation = new ReadOperation<>(new StringParamedSQL(sql.toString(), sqlParameterBinders), connectionProvider);
		readOperation.setValues(sqlArguments);
		
		return transformer.convert(readOperation.execute());
	}
	
	/**
	 * Sets a value for a SQL parameter. Not for Collection/Iterable value : see {@link #set(String, Iterable, Class)} dedicated method for it.
	 * No already-existing argument name checking is done, so you can overwrite/redefine an existing value. This lets you reexecute a Query with
	 * different parameters.
	 *
	 * @param paramName the name of the SQL parameter to be set
	 * @param value the value of the parameter
	 * @return this
	 * @see #set(String, Iterable, Class)
	 */
	public Query<T> set(String paramName, Object value) {
		return set(paramName, value, value == null ? null : (Class) value.getClass());
	}
	
	/**
	 * Sets a value for a SQL parameter. Not for Collection/Iterable value : see {@link #set(String, Iterable, Class)} dedicated method for it.
	 * No already-existing argument name checking is done, so you can overwrite/redefine an existing value. This lets you reexecute a Query with
	 * different parameters.
	 *
	 * @param paramName the name of the SQL parameter to be set
	 * @param value the value of the parameter
	 * @param valueType the content type of the {@link Iterable}, more exactly will determine which {@link ParameterBinder} to be used
	 * @return this
	 * @see #set(String, Iterable, Class)
	 */
	public <C> Query<T> set(String paramName, C value, Class<? super C> valueType) {
		sqlParameterBinders.put(paramName, value == null ? ALWAYS_SET_NULL_INSTANCE : parameterBinderProvider.getBinder(valueType));
		this.sqlArguments.put(paramName, value);
		return this;
	}
	
	/**
	 * Sets a value for a Collection/Iterable SQL argument. Must be distinguished from {@link #set(String, Object)} because real {@link Iterable}
	 * content type guessing can be difficult (or at least not accurate) and can lead to {@link Iterable} consumption.
	 * No already-existing argument name checking is done, so you can overwrite/redefine an existing value. This lets you reexecute a Query with
	 * different parameters.
	 *
	 * @param paramName the name of the SQL parameter to be set
	 * @param value the value of the parameter
	 * @param valueType the content type of the {@link Iterable}, more exactly will determine which {@link ParameterBinder} to be used
	 * @return this
	 */
	public <C> Query<T> set(String paramName, Iterable<C> value, Class<? super C> valueType) {
		this.sqlParameterBinders.put(paramName, value == null ? ALWAYS_SET_NULL_INSTANCE : parameterBinderProvider.getBinder(valueType));
		this.sqlArguments.put(paramName, value);
		return this;
	}
	
	public Query<T> remove(String paramName) {
		this.sqlParameterBinders.remove(paramName);
		this.sqlArguments.remove(paramName);
		return this;
	}
	
	/**
	 * An internal definition of a "column" : a selected column or a statement parameter
	 * @param <T> the value type of the "column"
	 */
	private static class Column<T> {
		
		private final String name;
		
		private final Class<T> valueType;
		
		private Column(String name, Class<T> valueType) {
			this.name = name;
			this.valueType = valueType;
		}
		
		public String getName() {
			return name;
		}
		
		public Class<T> getValueType() {
			return valueType;
		}
	}
	
	/**
	 * An internal class defining the way to instanciate a bean from a selected column
	 * @param <T> the bean type that will be created
	 * @param <I> the column value type which is also the input type of the bean factory
	 */
	private static class BeanCreationDefinition<T, I> {
		
		private final Column<I> column;
		
		private final SerializableFunction<I, T> factory;
		
		public BeanCreationDefinition(String columnName, Class<I> columnType, SerializableFunction<I, T> factory) {
			this.column = new Column<>(columnName, columnType);
			this.factory = factory;
		}
		
		public Column<I> getColumn() {
			return column;
		}
		
		public SerializableFunction<I, T> getFactory() {
			return factory;
		}
	}
	
	/**
	 * An internal class defining the way to map a result column to a bean "property" (more precisely a setter or whatever would take the value as input)
	 * @param <T> the bean type that supports the setter
	 * @param <I> the column value type which is also the input type of the property setter
	 */
	private static class ColumnMapping<T, I> {
		
		private final Column<I> column;
		
		private final SerializableBiConsumer<T, I> setter;
		
		public ColumnMapping(String columnName, Class<I> columnType, SerializableBiConsumer<T, I> setter) {
			this.column = new Column<>(columnName, columnType);
			this.setter = setter;
		}
		
		public Column<I> getColumn() {
			return column;
		}
		
		public SerializableBiConsumer<T, I> getSetter() {
			return setter;
		}
	}
}
