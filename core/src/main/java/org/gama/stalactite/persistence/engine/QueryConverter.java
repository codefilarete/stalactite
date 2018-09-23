package org.gama.stalactite.persistence.engine;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.danekja.java.util.function.serializable.SerializableBiConsumer;
import org.danekja.java.util.function.serializable.SerializableFunction;
import org.danekja.java.util.function.serializable.SerializableSupplier;
import org.gama.lang.bean.Converter;
import org.gama.lang.collection.Arrays;
import org.gama.reflection.MethodReferenceCapturer;
import org.gama.sql.ConnectionProvider;
import org.gama.sql.binder.ParameterBinder;
import org.gama.sql.binder.ParameterBinderProvider;
import org.gama.sql.dml.ReadOperation;
import org.gama.sql.dml.StringParamedSQL;
import org.gama.sql.result.ResultSetConverterSupport;
import org.gama.sql.result.ResultSetRowAssembler;
import org.gama.stalactite.persistence.structure.Table;

import static org.gama.sql.binder.NullAwareParameterBinder.ALWAYS_SET_NULL_INSTANCE;

/**
 * A class aimed at querying the database and creating Java beans from it.
 * Beans resulting of it are not expected to be used as entity and given to {@link Persister}, noticibly for insert
 * or update, because one should ensure that fields read from its query must be the same as those done by {@link Persister}, otherwise it may
 * result in column value erasure.
 * 
 * @author Guillaume Mary
 */
public class QueryConverter<C> {
	
	/** Default method capturer. Shared to cache result of each lookup. */
	private static final MethodReferenceCapturer METHOD_REFERENCE_CAPTURER = new MethodReferenceCapturer();
	
	/** The method capturer (when column types are not given by {@link #map(String, SerializableBiConsumer)}) */
	private final MethodReferenceCapturer methodReferenceCapturer;
	
	/** Type of bean that will be built by {@link #execute(ConnectionProvider)} */
	private final Class<C> rootBeanType;
	
	/** The sql that contains the select clause */
	private final CharSequence sql;
	
	/** The definition of root bean instanciation */
	private BeanCreationDefinition<C, ?> beanCreationDefinition;
	
	/** Mappings between selected columns and bean property setter */
	private final List<ColumnMapping> columnMappings = new ArrayList<>();
	
	private final List<ResultSetRowAssembler<C>> rawMappers = new ArrayList<>();
	
	/** The "registry" of {@link ParameterBinder}s, for column reading as well as sql argument setting */
	private final ParameterBinderProvider<Class> parameterBinderProvider;
	
	/** {@link ParameterBinder}s per ({@link java.sql.PreparedStatement}) parameter */
	private final Map<String, ParameterBinder> sqlParameterBinders = new HashMap<>();
	
	/** SQL argument values (for where clause, or anywhere else) */
	private final Map<String, Object> sqlArguments = new HashMap<>();
	
	/**
	 * Simple constructor
	 * @param rootBeanType type of built bean
	 * @param sql the sql to execute
	 * @param parameterBinderProvider a provider for SQL parameters and selected column
	 */
	public QueryConverter(Class<C> rootBeanType, CharSequence sql, ParameterBinderProvider<Class> parameterBinderProvider) {
		this(rootBeanType, sql, parameterBinderProvider, METHOD_REFERENCE_CAPTURER);
	}
	
	/**
	 * Constructor to share {@link MethodReferenceCapturer} between instance of {@link QueryConverter}
	 * @param rootBeanType type of built bean
	 * @param sql the sql to execute
	 * @param parameterBinderProvider a provider for SQL parameters and selected column
	 * @param methodReferenceCapturer a method capturer (when column types are not given by {@link #map(String, SerializableBiConsumer)}),
	 * default is {@link #METHOD_REFERENCE_CAPTURER}
	 */
	public QueryConverter(Class<C> rootBeanType, CharSequence sql, ParameterBinderProvider<Class> parameterBinderProvider, MethodReferenceCapturer methodReferenceCapturer) {
		this.rootBeanType = rootBeanType;
		this.sql = sql;
		this.parameterBinderProvider = parameterBinderProvider;
		this.methodReferenceCapturer = methodReferenceCapturer;
	}
	
	/**
	 * Defines the key column and the way to create the bean : a constructor with the key as parameter.
	 * 
	 * Note that the same method without columnType can't be written because it is in conflict with other mapKey(..) methods and/or available
	 * constructors in target bean class.
	 * 
	 * @param columnName the key column name
	 * @param factory the factory function that will instanciate new beans (with key as single argument)
	 * @param columnType the type of the column, which is also that of the factory argument
	 * @param <I> the type of the column, which is also that of the factory argument
	 * @return this
	 */
	public <I> QueryConverter<C> mapKey(String columnName, SerializableFunction<I, C> factory, Class<I> columnType) {
		this.beanCreationDefinition = new BeanCreationDefinition<>(columnName, factory, columnType);
		return this;
	}
	
	/**
	 * Same as {@link #mapKey(String, SerializableFunction, Class)} but with a non-argument constructor and a setter for key value.
	 * Reader of colum value will be deduced from setter by reflection.
	 *
	 * @param columnName the key column name
	 * @param javaBeanCtor the factory function that will instanciate new beans (no argument)
	 * @param keySetter setter for key
	 * @param <I> the type of the column
	 * @return this
	 */
	public <I> QueryConverter<C> mapKey(String columnName, SerializableSupplier<C> javaBeanCtor, SerializableBiConsumer<C, I> keySetter) {
		this.beanCreationDefinition = new BeanCreationDefinition<>(columnName, (SerializableFunction<I, C>) i -> {
			C newInstance = javaBeanCtor.get();
			keySetter.accept(newInstance, i);
			return newInstance;
		}, giveColumnType(keySetter));
		return this;
	}
	
	/**
	 * Same as {@link #mapKey(String, SerializableFunction, Class)} but with a non-argument constructor and a setter for key value.
	 *
	 * @param columnName the key column name
	 * @param javaBeanCtor the factory function that will instanciate new beans (no argument)
	 * @param keySetter setter for key
	 * @param columnType the type of the column
	 * @param <I> the type of the column
	 * @return this
	 */
	public <I> QueryConverter<C> mapKey(String columnName, SerializableSupplier<C> javaBeanCtor, SerializableBiConsumer<C, I> keySetter, Class<I> columnType) {
		return mapKey(columnName, (SerializableFunction<I, C>) i -> {
			C newInstance = javaBeanCtor.get();
			keySetter.accept(newInstance, i);
			return newInstance;
		}, columnType);
	}
	
	/**
	 * Defines a mapping between a column of the query and a bean property through its setter
	 *
	 * @param columnName a column name
	 * @param setter the setter function
	 * @param columnType the type of the column, which is also that of the setter argument
	 * @param <I> the type of the column, which is also that of the setter argument
	 * @return this
	 */
	public <I> QueryConverter<C> map(String columnName, SerializableBiConsumer<C, I> setter, Class<I> columnType) {
		add(new ColumnMapping<>(columnName, setter, columnType));
		return this;
	}
	
	/**
	 * Same as {@link #map(String, SerializableBiConsumer, Class)}.
	 * Differs by providing the possiblity to convert the value before setting it onto the bean.
	 * 
	 * @param columnName a column name
	 * @param setter the setter function
	 * @param columnType the type of the column, which is also that of the setter argument
	 * @param converter a converter of the read value from ResultSet
	 * @param <I> the type of the setter argument, which is also that of the converter result
	 * @param <J> the type of the column, which is also that of the converter argument
	 * @return this
	 */
	public <I, J> QueryConverter<C> map(String columnName, SerializableBiConsumer<C, J> setter, Class<I> columnType, SerializableFunction<I, J> converter) {
		return map(columnName, (t, i) -> setter.accept(t, converter.apply(i)), columnType);
	}
	
	/**
	 * Defines a mapping between a column of the query and a bean property through its setter.
	 * WARNING : Column type will be deduced from the setter type. To do it, some bytecode enhancement is required, therefore it's not the cleanest
	 * way to define the binding. Prefer {@link #map(String, SerializableBiConsumer, Class)}
	 *
	 * @param columnName a column name
	 * @param setter the setter function
	 * @param <I> the type of the column, which is also that of the setter argument
	 * @return this
	 */
	public <I> QueryConverter<C> map(String columnName, SerializableBiConsumer<C, I> setter) {
		map(columnName, setter, giveColumnType(setter));
		return this;
	}
	
	/**
	 * Same as {@link #map(String, SerializableBiConsumer)}.
	 * Differs by providing the possiblity to convert the value before setting it onto the bean.
	 *
	 * @param columnName a column name
	 * @param setter the setter function
	 * @param converter a converter of the read value from ResultSet
	 * @param <I> the type of the setter argument, which is also that of the converter result
	 * @param <J> the type of the column, which is also that of the converter argument
	 * @return this
	 */
	public <I, J> QueryConverter<C> map(String columnName, SerializableBiConsumer<C, J> setter, SerializableFunction<I, J> converter) {
		Method method = methodReferenceCapturer.findMethod(setter);
		Class<I> aClass = (Class<I>) method.getParameterTypes()[0];
		return map(columnName, (SerializableBiConsumer<C, I>) (t, i) -> setter.accept(t, converter.apply(i)), aClass);
	}
	
	/**
	 * Same as {@link #mapKey(String, SerializableFunction, Class)} but with {@link org.gama.stalactite.persistence.structure.Column} signature
	 * @param column the mapped column used as a key
	 * @param factory the bean constructor
	 * @param <I> type of the key
	 * @return this
	 */
	public <I> QueryConverter<C> mapKey(org.gama.stalactite.persistence.structure.Column<? extends Table, I> column, SerializableFunction<I, C> factory) {
		this.beanCreationDefinition = new BeanCreationDefinition<>(column.getName(), factory, column.getJavaType());
		return this;
	}
	
	/**
	 * Same as {@link #mapKey(String, SerializableSupplier, SerializableBiConsumer, Class)} but with {@link org.gama.stalactite.persistence.structure.Column} signature
	 * @param column the mapped column used as a key
	 * @param javaBeanCtor the factory function that will instanciate new beans (no argument)
	 * @param <I> type of the key
	 * @return this
	 */
	public <I> QueryConverter<C> mapKey(org.gama.stalactite.persistence.structure.Column<? extends Table, I> column, SerializableSupplier<C> javaBeanCtor, SerializableBiConsumer<C, I> keySetter) {
		return mapKey(column, (SerializableFunction<I, C>) i -> {
			C newInstance = javaBeanCtor.get();
			keySetter.accept(newInstance, i);
			return newInstance;
		});
	}
	
	/**
	 * Same as {@link #map(String, SerializableBiConsumer, Class)} but with {@link org.gama.stalactite.persistence.structure.Column} signature
	 * 
	 * @param column the mapped column
	 * @param setter the setter function
	 * @param <I> the type of the column, which is also that of the setter argument
	 * @return this
	 */
	public <I> QueryConverter<C> map(org.gama.stalactite.persistence.structure.Column<? extends Table, I> column, SerializableBiConsumer<C, I> setter) {
		map(column.getName(), setter, column.getJavaType());
		return this;
	}
	
	/**
	 * Same as {@link #map(org.gama.stalactite.persistence.structure.Column, SerializableBiConsumer)}.
	 * Differs by providing the possiblity to convert the value before setting it onto the bean.
	 *
	 * @param column the mapped column
	 * @param setter the setter function
	 * @param converter a converter of the read value from ResultSet
	 * @param <I> the type of the setter argument, which is also that of the converter result
	 * @param <J> the type of the column, which is also that of the converter argument
	 * @return this
	 */
	public <I, J> QueryConverter<C> map(org.gama.stalactite.persistence.structure.Column<? extends Table, I> column, SerializableBiConsumer<C, J> setter, Converter<I, J, RuntimeException> converter) {
		return map(column, (SerializableBiConsumer<C, I>) (t, i) -> setter.accept(t, converter.convert(i)));
	}
	
	private <I> void add(ColumnMapping<C, I> columnWire) {
		this.columnMappings.add(columnWire);
	}
	
	public QueryConverter<C> add(ResultSetRowAssembler<C> assembler) {
		rawMappers.add(assembler);
		return this;
	}
	
	/**
	 * Executes the query onto the connection given by the {@link ConnectionProvider}. Transforms the result to a list of beans thanks to the
	 * definition given through {@link #mapKey(String, SerializableFunction, Class)}, {@link #map(String, SerializableBiConsumer, Class)}
	 * and {@link #map(String, SerializableBiConsumer)} methods.
	 * 
	 * @param connectionProvider the object that will given the {@link java.sql.Connection}
	 * @return a {@link List} filled by the instances built
	 */
	public <I> List<C> execute(ConnectionProvider connectionProvider) {
		if (beanCreationDefinition == null) {
			throw new IllegalArgumentException("Bean creation is not defined, use mapKey(..)");
		}
		ResultSetConverterSupport<I, C> transformer = buildTransformer();
		
		StringParamedSQL parameterizedSQL = new StringParamedSQL(this.sql.toString(), sqlParameterBinders);
		try (ReadOperation<String> readOperation = new ReadOperation<>(parameterizedSQL, connectionProvider)) {
			readOperation.setValues(sqlArguments);
			
			return transformer.convert(readOperation.execute());
		}
	}
	
	protected <I> ResultSetConverterSupport<I, C> buildTransformer() {
		// creating ResultSetConverterSupport
		Column<I> keyColumn = (Column<I>) beanCreationDefinition.getColumn();
		Function<I, C> beanFactory = (Function<I, C>) beanCreationDefinition.getFactory();
		ParameterBinder<I> idParameterBinder = parameterBinderProvider.getBinder(keyColumn.getValueType());
		ResultSetConverterSupport<I, C> transformer = new ResultSetConverterSupport<>(rootBeanType, keyColumn.getName(), idParameterBinder, beanFactory);
		// adding complementary properties to transformer
		for (ColumnMapping<C, Object> columnMapping : columnMappings) {
			ParameterBinder parameterBinder = parameterBinderProvider.getBinder(columnMapping.getColumn().getValueType());
			transformer.add(columnMapping.getColumn().getName(), parameterBinder, columnMapping.getSetter());
		}
		rawMappers.forEach(transformer::add);
		return transformer;
	}
	
	private <I> Class<I> giveColumnType(SerializableBiConsumer<C, I> setter) {
		Method method = methodReferenceCapturer.findMethod(setter);
		// we could take the first parameter type, but with a particular syntax of setter it's insufficient, last element is better 
		return (Class<I>) Arrays.last(method.getParameterTypes());
	}
	
	/**
	 * Sets a value for a SQL parameter. Not for Collection/Iterable value : see {@link #set(String, Iterable, Class)} dedicated method for it.
	 * No already-existing argument name checking is done, so you can overwrite/redefine an existing value. This lets you reexecute a QueryConverter with
	 * different parameters.
	 *
	 * @param paramName the name of the SQL parameter to be set
	 * @param value the value of the parameter
	 * @return this
	 * @see #set(String, Iterable, Class)
	 * @see #clear(String)
	 */
	public QueryConverter<C> set(String paramName, Object value) {
		return set(paramName, value, value == null ? null : (Class) value.getClass());
	}
	
	/**
	 * Sets a value for a SQL parameter. Not for Collection/Iterable value : see {@link #set(String, Iterable, Class)} dedicated method for it.
	 * No already-existing argument name checking is done, so you can overwrite/redefine an existing value. This lets you reexecute a QueryConverter with
	 * different parameters.
	 *
	 * @param paramName the name of the SQL parameter to be set
	 * @param value the value of the parameter
	 * @param valueType the content type of the {@link Iterable}, more exactly will determine which {@link ParameterBinder} to be used
	 * @return this
	 * @see #set(String, Iterable, Class)
	 * @see #clear(String)
	 */
	public <O> QueryConverter<C> set(String paramName, O value, Class<? super O> valueType) {
		sqlParameterBinders.put(paramName, value == null ? ALWAYS_SET_NULL_INSTANCE : parameterBinderProvider.getBinder(valueType));
		this.sqlArguments.put(paramName, value);
		return this;
	}
	
	/**
	 * Sets a value for a Collection/Iterable SQL argument. Must be distinguished from {@link #set(String, Object)} because real {@link Iterable}
	 * content type guessing can be difficult (or at least not accurate) and can lead to {@link Iterable} consumption.
	 * No already-existing argument name checking is done, so you can overwrite/redefine an existing value. This lets you reexecute a QueryConverter with
	 * different parameters.
	 *
	 * @param paramName the name of the SQL parameter to be set
	 * @param value the value of the parameter
	 * @param valueType the content type of the {@link Iterable}, more exactly will determine which {@link ParameterBinder} to be used
	 * @return this
	 * @see #clear(String)
	 */
	public <O> QueryConverter<C> set(String paramName, Iterable<O> value, Class<? super O> valueType) {
		this.sqlParameterBinders.put(paramName, value == null ? ALWAYS_SET_NULL_INSTANCE : parameterBinderProvider.getBinder(valueType));
		this.sqlArguments.put(paramName, value);
		return this;
	}
	
	/**
	 * Remove the value of a parameter (previously set by set(..) methods)
	 * 
	 * @param paramName the name of the parameter to clear
	 * @return this
	 * @see #set(String, Object) 
	 */
	public QueryConverter<C> clear(String paramName) {
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
	private class BeanCreationDefinition<T, I> {
		
		private final Column<I> column;
		
		private final Function<I, T> factory;
		
		public BeanCreationDefinition(String columnName, Function<I, T> factory, Class<I> columnType) {
			this.column = new Column<>(columnName, columnType);
			this.factory = factory;
		}
		
		public Column<I> getColumn() {
			return column;
		}
		
		public Function<I, T> getFactory() {
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
		
		public ColumnMapping(String columnName, SerializableBiConsumer<T, I> setter, Class<I> columnType) {
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
