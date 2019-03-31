package org.gama.stalactite.persistence.engine;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;

import org.danekja.java.util.function.serializable.SerializableBiConsumer;
import org.danekja.java.util.function.serializable.SerializableBiFunction;
import org.danekja.java.util.function.serializable.SerializableFunction;
import org.danekja.java.util.function.serializable.SerializableSupplier;
import org.gama.lang.collection.Arrays;
import org.gama.lang.collection.Iterables;
import org.gama.lang.collection.KeepOrderSet;
import org.gama.lang.function.SerializableTriFunction;
import org.gama.lang.function.ThrowingConverter;
import org.gama.reflection.MethodReferenceCapturer;
import org.gama.sql.ConnectionProvider;
import org.gama.sql.binder.ParameterBinder;
import org.gama.sql.dml.ReadOperation;
import org.gama.sql.dml.StringParamedSQL;
import org.gama.sql.result.MultipleColumnsReader;
import org.gama.sql.result.ResultSetConverterSupport;
import org.gama.sql.result.ResultSetRowAssembler;
import org.gama.sql.result.ResultSetRowConverter;
import org.gama.sql.result.SingleColumnReader;
import org.gama.stalactite.persistence.sql.dml.binder.ColumnBinderRegistry;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.query.builder.SQLBuilder;

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
	
	/** The sql provider */
	private final SQLBuilder sql;
	
	/** The definition of root bean instanciation */
	private BeanCreationDefinition<?, C> beanCreationDefinition;
	
	/** Mappings between selected columns and bean property setter */
	private final List<ColumnMapping> columnMappings = new ArrayList<>();
	
	private final List<ResultSetRowAssembler<C>> rawMappers = new ArrayList<>();
	
	/** The registry of {@link ParameterBinder}s, for column reading as well as sql argument setting */
	private final ColumnBinderRegistry columnBinderRegistry;
	
	/** {@link ParameterBinder}s per ({@link java.sql.PreparedStatement}) parameter */
	private final Map<String, ParameterBinder> sqlParameterBinders = new HashMap<>();
	
	/** SQL argument values (for where clause, or anywhere else) */
	private final Map<String, Object> sqlArguments = new HashMap<>();
	
	/**
	 * Simple constructor
	 * 
	 * @param rootBeanType type of built bean
	 * @param sql the sql to execute
	 * @param columnBinderRegistry a provider for SQL parameters and selected column
	 */
	public QueryConverter(Class<C> rootBeanType, CharSequence sql, ColumnBinderRegistry columnBinderRegistry) {
		this(rootBeanType, sql, columnBinderRegistry, METHOD_REFERENCE_CAPTURER);
	}
	
	/**
	 * Simple constructor
	 *
	 * @param rootBeanType type of built bean
	 * @param sql the sql provider
	 * @param columnBinderRegistry a provider for SQL parameters and selected column
	 */
	public QueryConverter(Class<C> rootBeanType, SQLBuilder sql, ColumnBinderRegistry columnBinderRegistry) {
		this(rootBeanType, sql, columnBinderRegistry, METHOD_REFERENCE_CAPTURER);
	}
	
	/**
	 * Constructor to share {@link MethodReferenceCapturer} between instance of {@link QueryConverter}
	 * 
	 * @param rootBeanType type of built bean
	 * @param sql the sql to execute
	 * @param columnBinderRegistry a provider for SQL parameters and selected column
	 * @param methodReferenceCapturer a method capturer (when column types are not given by {@link #map(String, SerializableBiConsumer)}),
	 * default is {@link #METHOD_REFERENCE_CAPTURER}
	 */
	public QueryConverter(Class<C> rootBeanType, CharSequence sql, ColumnBinderRegistry columnBinderRegistry, MethodReferenceCapturer methodReferenceCapturer) {
		this(rootBeanType, () -> sql, columnBinderRegistry, methodReferenceCapturer);
	}
	
	/**
	 * Constructor to share {@link MethodReferenceCapturer} between instance of {@link QueryConverter}
	 *
	 * @param rootBeanType type of built bean
	 * @param sql the sql provider
	 * @param columnBinderRegistry a provider for SQL parameters and selected column
	 * @param methodReferenceCapturer a method capturer (when column types are not given by {@link #map(String, SerializableBiConsumer)}),
	 * default is {@link #METHOD_REFERENCE_CAPTURER}
	 */
	public QueryConverter(Class<C> rootBeanType, SQLBuilder sql, ColumnBinderRegistry columnBinderRegistry, MethodReferenceCapturer methodReferenceCapturer) {
		this.rootBeanType = rootBeanType;
		this.sql = sql;
		this.columnBinderRegistry = columnBinderRegistry;
		this.methodReferenceCapturer = methodReferenceCapturer;
	}
	
	/**
	 * Defines the key column and the way to create the bean : a constructor with the key as parameter.
	 *
	 * @param <I> the type of the column, which is also that of the factory argument
	 * @param factory the factory function that will instanciate new beans (with key as single argument)
	 * @param columnName the key column name
	 * @param columnType the type of the column, which is also that of the factory argument
	 * @return this
	 */
	public <I> QueryConverter<C> mapKey(SerializableFunction<I, C> factory, String columnName, Class<I> columnType) {
		this.beanCreationDefinition = new BeanCreationDefinition<>(factory, columnName, columnType);
		return this;
	}
	
	/**
	 * Defines key columns and the way to create the bean : a constructor with 2 arguments.
	 *
	 * @param <I> the type of the column, which is also that of the factory argument
	 * @param factory the factory function that will instanciate new beans (with key as single argument)
	 * @param column1Name the first key column name
	 * @param column1Type the type of the first column, which is also that of the factory first argument
	 * @param column2Name the second key column name
	 * @param column2Type the type of the second column, which is also that of the factory second argument
	 * @return this
	 */
	public <I, J> QueryConverter<C> mapKey(SerializableBiFunction<I, J, C> factory, String column1Name, Class<I> column1Type,
			String column2Name, Class<J> column2Type) {
		SerializableFunction<Object[], C> biArgsConstructorInvokation = args -> (C) factory.apply((I) args[0], (J) args[1]);
		this.beanCreationDefinition = new BeanCreationDefinition<>(biArgsConstructorInvokation,
				column1Name, column1Type,
				column2Name, column2Type);
		return this;
	}
	
	/**
	 * Defines key columns and the way to create the bean : a constructor with 3 arguments.
	 *
	 * @param <I> the type of the column, which is also that of the factory argument
	 * @param factory the factory function that will instanciate new beans (with key as single argument)
	 * @param column1Name the first key column name
	 * @param column1Type the type of the first column, which is also that of the factory first argument
	 * @param column2Name the second key column name
	 * @param column2Type the type of the second column, which is also that of the factory second argument
	 * @param column3Name the third key column name
	 * @param column3Type the type of the third column, which is also that of the factory third argument
	 * @return this
	 */
	public <I, J, K> QueryConverter<C> mapKey(SerializableTriFunction<I, J, K, C> factory, String column1Name, Class<I> column1Type,
			String column2Name, Class<J> column2Type,
			String column3Name, Class<K> column3Type) {
		SerializableFunction<Object[], C> triArgsConstructorInvokation = args -> (C) factory.apply((I) args[0], (J) args[1], (K) args[2]);
		this.beanCreationDefinition = new BeanCreationDefinition<>(triArgsConstructorInvokation,
				column1Name, column1Type,
				column2Name, column2Type,
				column3Name, column3Type);
		return this;
	}
	
	/**
	 * Same as {@link #mapKey(SerializableFunction, String, Class)} but with {@link org.gama.stalactite.persistence.structure.Column} signature
	 *
	 * @param <I> type of the key
	 * @param factory the factory function that will instanciate new beans (with key as single argument)
	 * @param column the mapped column used as a key
	 * @return this
	 */
	public <I> QueryConverter<C> mapKey(SerializableFunction<I, C> factory,
										org.gama.stalactite.persistence.structure.Column<? extends Table, I> column) {
		this.beanCreationDefinition = new BeanCreationDefinition<>(factory, column);
		return this;
	}
	
	/**
	 * Same as {@link #mapKey(SerializableFunction, org.gama.stalactite.persistence.structure.Column)} with a 2-args constructor
	 *
	 * @param <I> type of the key
	 * @param factory the factory function that will instanciate new beans (with key as single argument)
	 * @param column1 the first column of the key
	 * @param column2 the second column of the key
	 * @return this
	 */
	public <I, J> QueryConverter<C> mapKey(SerializableBiFunction<I, J, C> factory,
										   org.gama.stalactite.persistence.structure.Column<? extends Table, I> column1,
										   org.gama.stalactite.persistence.structure.Column<? extends Table, J> column2
	) {
		SerializableFunction<Object[], C> biArgsConstructorInvokation = args -> (C) factory.apply((I) args[0], (J) args[1]);
		this.beanCreationDefinition = new BeanCreationDefinition<>(biArgsConstructorInvokation, Arrays.asSet(column1, column2));
		return this;
	}
	
	/**
	 * Same as {@link #mapKey(SerializableFunction, org.gama.stalactite.persistence.structure.Column)} with a 3-args constructor
	 *
	 * @param <I> type of the key
	 * @param factory the factory function that will instanciate new beans (with key as single argument)
	 * @param column1 the first column of the key
	 * @param column2 the second column of the key
	 * @param column3 the third column of the key
	 * @return this
	 */
	public <I, J, K> QueryConverter<C> mapKey(SerializableTriFunction<I, J, K, C> factory,
											  org.gama.stalactite.persistence.structure.Column<? extends Table, I> column1,
											  org.gama.stalactite.persistence.structure.Column<? extends Table, J> column2,
											  org.gama.stalactite.persistence.structure.Column<? extends Table, K> column3
	) {
		SerializableFunction<Object[], C> triArgsConstructorInvokation = args -> (C) factory.apply((I) args[0], (J) args[1], (K) args[2]);
		this.beanCreationDefinition = new BeanCreationDefinition<>(triArgsConstructorInvokation, Arrays.asSet(column1, column2, column3));
		return this;
	}
	
	/**
	 * Same as {@link #mapKey(SerializableFunction, String, Class)} but with a non-argument constructor and a setter for key value.
	 * Reader of colum value will be deduced from setter by reflection.
	 *
	 * @param <I> the type of the column
	 * @param javaBeanCtor the factory function that will instanciate new beans (no argument)
	 * @param columnName the key column name
	 * @param keySetter setter for key
	 * @return this
	 */
	public <I> QueryConverter<C> mapKey(SerializableSupplier<C> javaBeanCtor, String columnName, SerializableBiConsumer<C, I> keySetter) {
		this.beanCreationDefinition = new BeanCreationDefinition<>(i -> javaBeanCtor.get(), columnName, giveColumnType(keySetter));
		map(columnName, keySetter);
		return this;
	}
	
	/**
	 * Same as {@link #mapKey(SerializableSupplier, String, SerializableBiConsumer)} but with {@link org.gama.stalactite.persistence.structure.Column} signature
	 * 
	 * @param <I> type of the key
	 * @param javaBeanCtor the factory function that will instanciate new beans (no argument)
	 * @param column the mapped column used as a key
	 * @return this
	 */
	public <I> QueryConverter<C> mapKey(SerializableSupplier<C> javaBeanCtor,
										org.gama.stalactite.persistence.structure.Column<? extends Table, I> column,
										SerializableBiConsumer<C, I> keySetter) {
		return mapKey(javaBeanCtor, column.getName(), keySetter);
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
		return map(columnName, (c, i) -> setter.accept(c, converter.apply(i)), columnType);
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
	 * @return this
	 */
	public <I> QueryConverter<C> map(String columnName, SerializableBiConsumer<C, I> setter, SerializableFunction<I, I> converter) {
		Method method = methodReferenceCapturer.findMethod(setter);
		Class<I> aClass = (Class<I>) method.getParameterTypes()[0];
		return map(columnName, (SerializableBiConsumer<C, I>) (c, i) -> setter.accept(c, converter.apply(i)), aClass);
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
		add(new ColumnMapping<>(column, setter));
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
	public <I, J> QueryConverter<C> map(org.gama.stalactite.persistence.structure.Column<? extends Table, I> column, SerializableBiConsumer<C, J> setter,
										ThrowingConverter<I, J, RuntimeException> converter) {
		return map(column, (SerializableBiConsumer<C, I>) (c, i) -> setter.accept(c, converter.convert(i)));
	}
	
	private <I> void add(ColumnMapping<C, I> columnMapping) {
		this.columnMappings.add(columnMapping);
	}
	
	public QueryConverter<C> add(ResultSetRowAssembler<C> assembler) {
		rawMappers.add(assembler);
		return this;
	}
	
	/**
	 * Executes the query onto the connection given by the {@link ConnectionProvider}. Transforms the result to a list of beans thanks to the
	 * definition given through {@link #mapKey(SerializableFunction, String, Class)}, {@link #map(String, SerializableBiConsumer, Class)}
	 * and {@link #map(String, SerializableBiConsumer)} methods.
	 *
	 * @param connectionProvider the object that will given the {@link java.sql.Connection}
	 * @return a {@link List} filled by the instances built
	 */
	public List<C> execute(ConnectionProvider connectionProvider) {
		if (beanCreationDefinition == null) {
			throw new IllegalArgumentException("Bean creation is not defined, use mapKey(..)");
		}
		ResultSetConverterSupport<?, C> transformer = buildTransformer();
		
		StringParamedSQL parameterizedSQL = new StringParamedSQL(this.sql.toSQL().toString(), sqlParameterBinders);
		try (ReadOperation<String> readOperation = new ReadOperation<>(parameterizedSQL, connectionProvider)) {
			readOperation.setValues(sqlArguments);
			
			return transformer.convert(readOperation.execute());
		}
	}
	
	private ResultSetConverterSupport<?, C> buildTransformer() {
		// creating ResultSetConverter
		ResultSetConverterSupport<?, C> transformer;
		Set<Column> columns = beanCreationDefinition.getColumns();
		if (columns.size() == 1) {
			transformer = buildSingleColumnKeyTransformer((Column<Object>) Iterables.first(columns));
		} else {
			transformer = buildComposedKeyTransformer(columns);
		}
		// adding complementary properties to transformer
		for (ColumnMapping<C, Object> columnMapping : columnMappings) {
			ParameterBinder parameterBinder = columnMapping.getColumn().getBinder();
			transformer.add(columnMapping.getColumn().getName(), parameterBinder, columnMapping.getSetter());
		}
		rawMappers.forEach(transformer::add);
		return transformer;
	}
	
	private ResultSetConverterSupport<?, C> buildSingleColumnKeyTransformer(Column<Object> keyColumn) {
		ParameterBinder<Object> idParameterBinder = keyColumn.getBinder();
		return new ResultSetConverterSupport<>(rootBeanType, keyColumn.getName(), idParameterBinder,
				(SerializableFunction<Object, C>) beanCreationDefinition.getFactory());
	}
	
	private ResultSetConverterSupport<?, C> buildComposedKeyTransformer(Set<Column> columns) {
		Set<SingleColumnReader> columnReaders = Iterables.collect(columns, c -> {
			ParameterBinder reader = c.getBinder();
			return new SingleColumnReader<>(c.getName(), reader);
		}, HashSet::new);
		MultipleColumnsReader<Object[]> multipleColumnsReader = new MultipleColumnsReader<>(columnReaders, resultSetRow -> {
			// we transform all columns value into a Object[]
			Object[] contructorArgs = new Object[columns.size()];
			int i = 0;
			for (Column column : columns) {
				contructorArgs[i++] = resultSetRow.get(column.getName());
			}
			return contructorArgs;
		});
		Function<Object[], C> beanFactory = (Function<Object[], C>) beanCreationDefinition.getFactory();
		
		ResultSetRowConverter<Object[], C> resultSetRowConverter = new ResultSetRowConverter<>(
				rootBeanType,
				multipleColumnsReader,
				beanFactory);
		return new ResultSetConverterSupport<>(resultSetRowConverter);
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
		sqlParameterBinders.put(paramName, value == null ? ALWAYS_SET_NULL_INSTANCE : columnBinderRegistry.getBinder(valueType));
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
		this.sqlParameterBinders.put(paramName, value == null ? ALWAYS_SET_NULL_INSTANCE : columnBinderRegistry.getBinder(valueType));
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
	private interface Column<T> {
		
		String getName();
		
		ParameterBinder<T> getBinder();
	}
	
	private class StatementParameter<T> implements QueryConverter.Column<T> {
		
		private final String name;
		
		private final Class<T> valueType;
		
		private StatementParameter(String name, Class<T> valueType) {
			this.name = name;
			this.valueType = valueType;
		}
		
		public String getName() {
			return name;
		}
		
		public ParameterBinder<T> getBinder() {
			return columnBinderRegistry.getBinder(valueType);
		}
	}
	
	private class ColumnWrapper<T> implements QueryConverter.Column<T> {
		
		private final org.gama.stalactite.persistence.structure.Column<?, T> column;
		
		private ColumnWrapper(org.gama.stalactite.persistence.structure.Column<?, T> column) {
			this.column = column;
		}
		
		public String getName() {
			return column.getName();
		}
		
		public ParameterBinder<T> getBinder() {
			return columnBinderRegistry.getBinder(column);
		}
	}
	
	/**
	 * An internal class defining the way to instanciate a bean from a selected column
	 * @param <O> the bean type that will be created
	 * @param <I> the column value type which is also the input type of the bean factory
	 */
	private class BeanCreationDefinition<I, O> {
		
		private final KeepOrderSet<Column> columns = new KeepOrderSet<>();
		
		private final SerializableFunction<I, O> factory;
		
		/**
		 * Constructor for bean identified by a single column (not composed primary key)
		 * 
		 * @param factory bean factory, a constructor reference for instance (with single parameter)
		 * @param columnName name of the column that contains bean key
		 * @param columnType column type, must be the same as factory input
		 */
		public BeanCreationDefinition(SerializableFunction<I, O> factory, String columnName, Class<I> columnType) {
			this.columns.add(new StatementParameter<>(columnName, columnType));
			this.factory = factory;
		}
		
		public BeanCreationDefinition(SerializableFunction<I, O> factory, String column1Name, Class column1Type,
										  String column2Name, Class column2Type) {
			this(factory, column1Name, column1Type);
			this.columns.add(new StatementParameter<>(column2Name, column2Type));
		}
		
		public BeanCreationDefinition(SerializableFunction<I, O> factory, String column1Name, Class column1Type,
									  String column2Name, Class column2Type,
									  String column3Name, Class column3Type) {
			this(factory, column1Name, column1Type, column2Name, column2Type);
			this.columns.add(new StatementParameter<>(column3Name, column3Type));
		}

		public BeanCreationDefinition(SerializableFunction<I, O> factory, org.gama.stalactite.persistence.structure.Column<?, I> column) {
			this.columns.add(new ColumnWrapper<>(column));
			this.factory = factory;
		}
		
		public BeanCreationDefinition(SerializableFunction<I, O> factory, Set<org.gama.stalactite.persistence.structure.Column> columns
		) {
			this.columns.addAll(Iterables.collect(columns, ColumnWrapper::new, (Supplier<Set<Column>>) KeepOrderSet::new));
			this.factory = factory;
		}
		
		public Set<QueryConverter.Column> getColumns() {
			return columns;
		}
		
		public SerializableFunction<I, O> getFactory() {
			return factory;
		}
		
	}
	
	/**
	 * An internal class defining the way to map a result column to a bean "property" (more precisely a setter or whatever would take the value as input)
	 * @param <T> the bean type that supports the setter
	 * @param <I> the column value type which is also the input type of the property setter
	 */
	private class ColumnMapping<T, I> {
		
		private final QueryConverter.Column<I> column;
		
		private final SerializableBiConsumer<T, I> setter;
		
		public ColumnMapping(String columnName, SerializableBiConsumer<T, I> setter, Class<I> columnType) {
			this.column = new StatementParameter<>(columnName, columnType);
			this.setter = setter;
		}
		
		public ColumnMapping(org.gama.stalactite.persistence.structure.Column<?, I> column, SerializableBiConsumer<T, I> setter) {
			this.column = new ColumnWrapper<>(column);
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
