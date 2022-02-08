package org.gama.stalactite.persistence.engine;

import java.lang.reflect.Executable;
import java.lang.reflect.Method;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;

import org.danekja.java.util.function.serializable.SerializableBiConsumer;
import org.danekja.java.util.function.serializable.SerializableBiFunction;
import org.danekja.java.util.function.serializable.SerializableFunction;
import org.danekja.java.util.function.serializable.SerializableSupplier;
import org.codefilarete.tool.Reflections;
import org.codefilarete.tool.collection.Arrays;
import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.tool.function.Converter;
import org.codefilarete.tool.function.SerializableTriFunction;
import org.codefilarete.reflection.MethodReferenceCapturer;
import org.gama.stalactite.sql.result.BeanRelationFixer;
import org.gama.stalactite.persistence.engine.runtime.Persister;
import org.gama.stalactite.persistence.sql.dml.binder.ColumnBinderRegistry;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.query.builder.SQLBuilder;
import org.gama.stalactite.sql.ConnectionProvider;
import org.gama.stalactite.sql.binder.ParameterBinder;
import org.gama.stalactite.sql.dml.ReadOperation;
import org.gama.stalactite.sql.dml.StringParamedSQL;
import org.gama.stalactite.sql.result.MultipleColumnsReader;
import org.gama.stalactite.sql.result.ResultSetIterator;
import org.gama.stalactite.sql.result.ResultSetRowAssembler;
import org.gama.stalactite.sql.result.ResultSetRowTransformer;
import org.gama.stalactite.sql.result.SingleColumnReader;
import org.gama.stalactite.sql.result.WholeResultSetTransformer;
import org.gama.stalactite.sql.result.WholeResultSetTransformer.AssemblyPolicy;

import static org.gama.stalactite.sql.binder.NullAwareParameterBinder.ALWAYS_SET_NULL_INSTANCE;

/**
 * A class aimed at querying the database and creating Java beans from it.
 * Beans resulting of it are not expected to be used as entity and given to {@link Persister}, noticibly for insert
 * or update, because one should ensure that fields read from its query must be the same as those done by {@link Persister}, otherwise it may
 * result in column value erasure.
 * 
 * @author Guillaume Mary
 */
public class QueryMapper<C> implements BeanKeyQueryMapper<C>, BeanPropertyQueryMapper<C> {
	
	/** Default method capturer. Shared to cache result of each lookup. */
	private static final MethodReferenceCapturer METHOD_REFERENCE_CAPTURER = new MethodReferenceCapturer();
	
	/** The method capturer (when column types are not given by {@link #map(String, SerializableBiConsumer)}) */
	private final MethodReferenceCapturer methodReferenceCapturer;
	
	/** Type of bean that will be built by {@link #execute(ConnectionProvider)} */
	private final Class<C> rootBeanType;
	
	/** The sql provider */
	private final SQLBuilder sql;
	
	/** The registry of {@link ParameterBinder}s, for column reading as well as sql argument setting */
	private final ColumnBinderRegistry columnBinderRegistry;
	
	/** {@link ParameterBinder}s per ({@link java.sql.PreparedStatement}) parameter */
	private final Map<String, ParameterBinder> sqlParameterBinders = new HashMap<>();
	
	/** SQL argument values (for where clause, or anywhere else) */
	private final Map<String, Object> sqlArguments = new HashMap<>();
	
	/** Delegate for {@link java.sql.ResultSet} transformation, will get all the mapping configuration */
	private WholeResultSetTransformer<?, C> rootTransformer;
	
	private final QueryMapping<C, ?> mapping = new QueryMapping<>();
	
	/**
	 * Simple constructor
	 * 
	 * @param rootBeanType type of built bean
	 * @param sql the sql to execute
	 * @param columnBinderRegistry a provider for SQL parameters and selected column
	 */
	public QueryMapper(Class<C> rootBeanType, CharSequence sql, ColumnBinderRegistry columnBinderRegistry) {
		this(rootBeanType, sql, columnBinderRegistry, METHOD_REFERENCE_CAPTURER);
	}
	
	/**
	 * Simple constructor
	 *
	 * @param rootBeanType type of built bean
	 * @param sql the sql provider
	 * @param columnBinderRegistry a provider for SQL parameters and selected column
	 */
	public QueryMapper(Class<C> rootBeanType, SQLBuilder sql, ColumnBinderRegistry columnBinderRegistry) {
		this(rootBeanType, sql, columnBinderRegistry, METHOD_REFERENCE_CAPTURER);
	}
	
	/**
	 * Constructor to share {@link MethodReferenceCapturer} between instance of {@link QueryMapper}
	 * 
	 * @param rootBeanType type of built bean
	 * @param sql the sql to execute
	 * @param columnBinderRegistry a provider for SQL parameters and selected column
	 * @param methodReferenceCapturer a method capturer (when column types are not given by {@link #map(String, SerializableBiConsumer)}),
	 * default is {@link #METHOD_REFERENCE_CAPTURER}
	 */
	public QueryMapper(Class<C> rootBeanType, CharSequence sql, ColumnBinderRegistry columnBinderRegistry, MethodReferenceCapturer methodReferenceCapturer) {
		this(rootBeanType, () -> sql, columnBinderRegistry, methodReferenceCapturer);
	}
	
	/**
	 * Constructor to share {@link MethodReferenceCapturer} between instance of {@link QueryMapper}
	 *
	 * @param rootBeanType type of built bean
	 * @param sql the sql provider
	 * @param columnBinderRegistry a provider for SQL parameters and selected column
	 * @param methodReferenceCapturer a method capturer (when column types are not given by {@link #map(String, SerializableBiConsumer)}),
	 * default is {@link #METHOD_REFERENCE_CAPTURER}
	 */
	public QueryMapper(Class<C> rootBeanType, SQLBuilder sql, ColumnBinderRegistry columnBinderRegistry, MethodReferenceCapturer methodReferenceCapturer) {
		this.rootBeanType = rootBeanType;
		this.sql = sql;
		this.columnBinderRegistry = columnBinderRegistry;
		this.methodReferenceCapturer = methodReferenceCapturer;
	}
	
	@Override
	public QueryMapper<C> mapKey(SerializableSupplier<C> factory) {
		this.rootTransformer = new WholeResultSetTransformer<>(rootBeanType, factory);
		return this;
	}
	
	@Override
	public <I> QueryMapper<C> mapKey(SerializableFunction<I, C> factory, String columnName) {
		MethodReferenceCapturer methodReferenceCapturer = new MethodReferenceCapturer();
		// Looking for column type (necessary to know how to read ResultSet) by looking to first argument of given function
		// (which can be either a constructor or a method factory) 
		Executable executable = methodReferenceCapturer.findExecutable(factory);
		this.rootTransformer = buildSingleColumnKeyTransformer(new ColumnDefinition<>(columnName, (Class<I>) executable.getParameterTypes()[0]), factory);
		return this;
	}
	
	@Override
	public <I, J> QueryMapper<C> mapKey(SerializableBiFunction<I, J, C> factory, String column1, String column2) {
		MethodReferenceCapturer methodReferenceCapturer = new MethodReferenceCapturer();
		// Looking for column type (necessary to know how to read ResultSet) by looking to first argument of given function
		// (which can be either a constructor or a method factory) 
		Executable executable = methodReferenceCapturer.findExecutable(factory);
		SerializableFunction<Object[], C> constructorInvokation = args -> (C) factory.apply((I) args[0], (J) args[1]);
		this.rootTransformer = buildComposedKeyTransformer(Arrays.asSet(
				new ColumnDefinition<>(column1, executable.getParameterTypes()[0]),
				new ColumnDefinition<>(column2, executable.getParameterTypes()[1])),
				constructorInvokation);
		return this;
	}
	
	@Override
	public <I, J, K> QueryMapper<C> mapKey(SerializableTriFunction<I, J, K, C> factory, String column1, String column2, String column3) {
		MethodReferenceCapturer methodReferenceCapturer = new MethodReferenceCapturer();
		Executable executable = methodReferenceCapturer.findExecutable(factory);
		// Looking for column type (necessary to know how to read ResultSet) by looking to first argument of given function
		// (which can be either a constructor or a method factory) 
		SerializableFunction<Object[], C> constructorInvokation = args -> (C) factory.apply((I) args[0], (J) args[1], (K) args[2]);
		this.rootTransformer = buildComposedKeyTransformer(Arrays.asSet(
				new ColumnDefinition<>(column1, executable.getParameterTypes()[0]),
				new ColumnDefinition<>(column2, executable.getParameterTypes()[1]),
				new ColumnDefinition<>(column3, executable.getParameterTypes()[2])),
				constructorInvokation);
		return this;
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
	@Override
	public <I> QueryMapper<C> mapKey(SerializableFunction<I, C> factory, String columnName, Class<I> columnType) {
		this.rootTransformer = buildSingleColumnKeyTransformer(new ColumnDefinition<>(columnName, columnType), factory);
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
	@Override
	public <I, J> QueryMapper<C> mapKey(SerializableBiFunction<I, J, C> factory, String column1Name, Class<I> column1Type,
										String column2Name, Class<J> column2Type) {
		SerializableFunction<Object[], C> constructorInvokation = args -> (C) factory.apply((I) args[0], (J) args[1]);
		this.rootTransformer = buildComposedKeyTransformer(Arrays.asSet(
				new ColumnDefinition<>(column1Name, column1Type),
				new ColumnDefinition<>(column2Name, column2Type)),
				constructorInvokation);
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
	@Override
	public <I, J, K> QueryMapper<C> mapKey(SerializableTriFunction<I, J, K, C> factory, String column1Name, Class<I> column1Type,
										   String column2Name, Class<J> column2Type,
										   String column3Name, Class<K> column3Type) {
		SerializableFunction<Object[], C> constructorInvokation = args -> (C) factory.apply((I) args[0], (J) args[1], (K) args[2]);
		this.rootTransformer = buildComposedKeyTransformer(Arrays.asSet(
				new ColumnDefinition<>(column1Name, column1Type),
				new ColumnDefinition<>(column2Name, column2Type),
				new ColumnDefinition<>(column3Name, column3Type)),
				constructorInvokation);
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
	@Override
	public <I> QueryMapper<C> mapKey(SerializableFunction<I, C> factory,
									 org.gama.stalactite.persistence.structure.Column<? extends Table, I> column) {
		this.rootTransformer = buildSingleColumnKeyTransformer(new ColumnWrapper<>(column), factory);
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
	@Override
	public <I, J> QueryMapper<C> mapKey(SerializableBiFunction<I, J, C> factory,
										org.gama.stalactite.persistence.structure.Column<? extends Table, I> column1,
										org.gama.stalactite.persistence.structure.Column<? extends Table, J> column2
	) {
		SerializableFunction<Object[], C> constructorInvokation = args -> (C) factory.apply((I) args[0], (J) args[1]);
		this.rootTransformer = buildComposedKeyTransformer(Arrays.asSet(
				new ColumnWrapper<>(column1),
				new ColumnWrapper<>(column2)),
				constructorInvokation);
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
	@Override
	public <I, J, K> QueryMapper<C> mapKey(SerializableTriFunction<I, J, K, C> factory,
										   org.gama.stalactite.persistence.structure.Column<? extends Table, I> column1,
										   org.gama.stalactite.persistence.structure.Column<? extends Table, J> column2,
										   org.gama.stalactite.persistence.structure.Column<? extends Table, K> column3
	) {
		SerializableFunction<Object[], C> constructorInvokation = args -> (C) factory.apply((I) args[0], (J) args[1], (K) args[2]);
		this.rootTransformer = buildComposedKeyTransformer(Arrays.asSet(
				new ColumnWrapper<>(column1),
				new ColumnWrapper<>(column2),
				new ColumnWrapper<>(column3)),
				constructorInvokation);
		return this;
	}
	
	@Override
	public <I> QueryMapper<I> mapKey(String columnName, Class<I> columnType) {
		this.rootTransformer = (WholeResultSetTransformer<?, C>) buildSingleColumnKeyTransformer(new ColumnDefinition<>(columnName, columnType), SerializableFunction.identity());
		return (QueryMapper<I>) this;
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
	@Override
	public <I> QueryMapper<C> map(String columnName, SerializableBiConsumer<C, I> setter, Class<I> columnType) {
		map(new ColumnMapping<>(columnName, setter, columnType));
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
	@Override
	public <I, J> QueryMapper<C> map(String columnName, SerializableBiConsumer<C, J> setter, Class<I> columnType, Converter<I, J> converter) {
		return map(columnName, (c, i) -> setter.accept(c, converter.convert(i)), columnType);
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
	@Override
	public <I> QueryMapper<C> map(String columnName, SerializableBiConsumer<C, I> setter) {
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
	@Override
	public <I, J> QueryMapper<C> map(String columnName, SerializableBiConsumer<C, J> setter, Converter<I, J> converter) {
		Method method = methodReferenceCapturer.findMethod(setter);
		Class<I> aClass = (Class<I>) method.getParameterTypes()[0];
		return map(columnName, (SerializableBiConsumer<C, I>) (c, i) -> setter.accept(c, converter.convert(i)), aClass);
	}
	
	/**
	 * Same as {@link #map(String, SerializableBiConsumer, Class)} but with {@link org.gama.stalactite.persistence.structure.Column} signature
	 *
	 * @param column the mapped column
	 * @param setter the setter function
	 * @param <I> the type of the column, which is also that of the setter argument
	 * @return this
	 */
	@Override
	public <I> QueryMapper<C> map(org.gama.stalactite.persistence.structure.Column<? extends Table, I> column, SerializableBiConsumer<C, I> setter) {
		map(new ColumnMapping<>(column, setter));
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
	@Override
	public <I, J> QueryMapper<C> map(org.gama.stalactite.persistence.structure.Column<? extends Table, I> column,
									 SerializableBiConsumer<C, J> setter,
									 Converter<I, J> converter) {
		return map(column, (SerializableBiConsumer<C, I>) (c, i) -> setter.accept(c, converter.convert(i)));
	}
	
	private <I> void map(ColumnMapping<C, I> columnMapping) {
		mapping.add((ColumnMapping) columnMapping);
	}
	
	/** Overriden to adapt return type */
	@Override
	public QueryMapper<C> map(ResultSetRowAssembler<C> assembler) {
		return map(assembler, AssemblyPolicy.ON_EACH_ROW);
	}
	
	@Override
	public QueryMapper<C> map(ResultSetRowAssembler<C> assembler, AssemblyPolicy assemblyPolicy) {
		mapping.add(assembler, assemblyPolicy);
		return this;
	}
	
	@Override
	public <K, V> QueryMapper<C> map(BeanRelationFixer<C, V> combiner, ResultSetRowTransformer<K, V> relatedBeanCreator) {
		mapping.add((BeanRelationFixer) combiner, relatedBeanCreator);
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
		return execute(WholeResultSetTransformer::transformAll, connectionProvider);
	}
	
	/**
	 * Executes the query onto the connection given by the {@link ConnectionProvider}. Transforms the result to a bean thanks to the
	 * definition given through {@link #mapKey(SerializableFunction, String, Class)}, {@link #map(String, SerializableBiConsumer, Class)}
	 * and {@link #map(String, SerializableBiConsumer)} methods.
	 *
	 * @param connectionProvider the object that will given the {@link java.sql.Connection}
	 * @return an object, maybe null
	 */
	public C executeUnique(ConnectionProvider connectionProvider) {
		return execute((transformerToUse, resultSet) -> {
			// mimicing what WholeResultSetTransformer.transformAll does, but with only first ResultSet Row
			ResultSetIterator<C> resultSetIterator = new ResultSetIterator<C>(resultSet) {
				@Override
				public C convert(ResultSet resultSet) throws SQLException {
					return transformerToUse.transform(resultSet);
				}
			};
			// we transform only first result since method is expected to return it
			return transformerToUse.doWithBeanCache(() -> resultSetIterator.hasNext() ? resultSetIterator.next() : null);
		}, connectionProvider);
	}
	
	/**
	 * Method that mutualizes code of {@link #execute(ConnectionProvider)} and {@link #executeUnique(ConnectionProvider)}
	 */
	private <O> O execute(BiFunction<WholeResultSetTransformer<?, C>, ResultSet, O> code, ConnectionProvider connectionProvider) {
		WholeResultSetTransformer<?, C> transformerToUse;
		if (rootTransformer == null) {
			transformerToUse = new WholeResultSetTransformer<>(rootBeanType, () -> Reflections.newInstance(rootBeanType));
		} else {
			transformerToUse = rootTransformer;
		}
		this.mapping.applyTo((WholeResultSetTransformer) transformerToUse);
		StringParamedSQL parameterizedSQL = new StringParamedSQL(this.sql.toSQL().toString(), sqlParameterBinders);
		try (ReadOperation<String> readOperation = new ReadOperation<>(parameterizedSQL, connectionProvider)) {
			readOperation.setValues(sqlArguments);
			return code.apply(transformerToUse, readOperation.execute());
		}
	}
	
	private <I, O> WholeResultSetTransformer<I, O> buildSingleColumnKeyTransformer(Column<I> keyColumn, SerializableFunction<I, O> beanFactory) {
		return new WholeResultSetTransformer<>((Class<O>) rootBeanType, keyColumn.getName(), keyColumn.getBinder(), beanFactory);
	}
	
	private WholeResultSetTransformer<Object[], C> buildComposedKeyTransformer(Set<Column> columns, SerializableFunction<Object[], C> beanFactory) {
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
		
		ResultSetRowTransformer<Object[], C> resultSetRowConverter = new ResultSetRowTransformer<>(
				rootBeanType,
				multipleColumnsReader,
				beanFactory);
		return new WholeResultSetTransformer<>(resultSetRowConverter);
	}
	
	private <I> Class<I> giveColumnType(SerializableBiConsumer<C, I> setter) {
		Method method = methodReferenceCapturer.findMethod(setter);
		// we could take the first parameter type, but with a particular syntax of setter it's insufficient, last element is better 
		return (Class<I>) Arrays.last(method.getParameterTypes());
	}
	
	/**
	 * Sets a value for a SQL parameter. Not to be used with Collection/Iterable value : see {@link #set(String, Iterable, Class)} dedicated method for it.
	 * No check for "already set" argument is done, so one can overwrite/redefine an existing value. This lets one reexecutes a
	 * {@link QueryMapper} with different parameters.
	 *
	 * @param paramName the name of the SQL parameter to be set
	 * @param value the value of the parameter, null is authorized but since type can't be know in this case {@link java.sql.PreparedStatement#setObject(int, Object)}
	 * 				will be used, so prefer {@link #set(String, Object, Class)} if your database driver doesn't support well setObject(..),
	 * 			    see	{@link org.gama.stalactite.sql.binder.NullAwareParameterBinder#ALWAYS_SET_NULL_INSTANCE}
	 * @return this
	 * @see #set(String, Iterable, Class)
	 */
	public QueryMapper<C> set(String paramName, Object value) {
		return set(paramName, value, value == null ? null : (Class) value.getClass());
	}
	
	/**
	 * Sets a value for a SQL parameter. Not for Collection/Iterable value : see {@link #set(String, Iterable, Class)} dedicated method for it.
	 * No check for "already set" argument is done, so one can overwrite/redefine an existing value. This lets one reexecutes a
	 * {@link QueryMapper} with different parameters.
	 *
	 * @param paramName the name of the SQL parameter to be set
	 * @param value the value of the parameter
	 * @param valueType the content type of the {@link Iterable}, more exactly will determine which {@link ParameterBinder} to be used
	 * @return this
	 * @see #set(String, Iterable, Class)
	 */
	public <O> QueryMapper<C> set(String paramName, O value, Class<? super O> valueType) {
		this.sqlParameterBinders.put(paramName, value == null ? ALWAYS_SET_NULL_INSTANCE : columnBinderRegistry.getBinder(valueType));
		this.sqlArguments.put(paramName, value);
		return this;
	}
	
	/**
	 * Sets a value for a Collection/Iterable SQL argument. Must be distinguished from {@link #set(String, Object)} because real {@link Iterable}
	 * content type guessing can be difficult (or at least not accurate) and can lead to {@link Iterable} consumption.
	 * No check for "already set" argument is done, so one can overwrite/redefine an existing value. This lets one reexecutes a
	 * {@link QueryMapper} with different parameters.
	 *
	 * @param paramName the name of the SQL parameter to be set
	 * @param value the value of the parameter
	 * @param valueType the content type of the {@link Iterable}, more exactly will determine which {@link ParameterBinder} to be used
	 * @return this
	 */
	public <O> QueryMapper<C> set(String paramName, Iterable<O> value, Class<? super O> valueType) {
		this.sqlParameterBinders.put(paramName, value == null ? ALWAYS_SET_NULL_INSTANCE : columnBinderRegistry.getBinder(valueType));
		this.sqlArguments.put(paramName, value);
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
	
	private class ColumnDefinition<T> implements QueryMapper.Column<T> {
		
		private final String name;
		
		private final Class<T> valueType;
		
		private ColumnDefinition(String name, Class<T> valueType) {
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
	
	private class ColumnWrapper<T> implements QueryMapper.Column<T> {
		
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
	 * An internal class defining the way to map a result column to a bean "property" (more precisely a setter or whatever which takes the value as input)
	 * 
	 * @param <T> the bean type that supports the setter
	 * @param <I> the column value type which is also the input type of the property setter
	 */
	private class ColumnMapping<T, I> {
		
		private final QueryMapper.Column<I> column;
		
		private final SerializableBiConsumer<T, I> setter;
		
		public ColumnMapping(String columnName, SerializableBiConsumer<T, I> setter, Class<I> columnType) {
			this.column = new ColumnDefinition<>(columnName, columnType);
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
	
	
	/**
	 * Stores configuration defined by user to let user uses {@link QueryMapper} methods in any order : without it mapKey(..) should be used first
	 * because we might have to apply mapping to {@link WholeResultSetTransformer} instance
	 * 
	 * @param <C> created instance type
	 * @param <I> identifier type
	 */
	private static class QueryMapping<C, I> {
		
		private final List<Mapping> mappings = new ArrayList<>(10);
		
		public void add(QueryMapper<C>.ColumnMapping<C, I> columnMapping) {
			mappings.add(new ColumnMapping(columnMapping));
		}
		
		public void add(ResultSetRowAssembler<C> assembler, AssemblyPolicy assemblyPolicy) {
			mappings.add(new AssemblerMapping(assembler, assemblyPolicy));
		}
		
		public <K, V> void add(BeanRelationFixer<K, V> combiner, ResultSetRowTransformer<K, V> relatedBeanCreator) {
			mappings.add(new RelationMapping(combiner, relatedBeanCreator));
		}
		
		/**
		 * Transfert this mapping to given instance
		 * @param target instance that will consume current mapping
		 */
		public void applyTo(WholeResultSetTransformer<I, C> target) {
			this.mappings.forEach(m -> {
				if (m instanceof ColumnMapping) {
					QueryMapper.ColumnMapping columnMapping = ((ColumnMapping) m).columnMapping;
					target.add(columnMapping.getColumn().getName(), columnMapping.getColumn().getBinder(), columnMapping.getSetter());
				} else if (m instanceof AssemblerMapping) {
					AssemblerMapping assemblerMapping = (AssemblerMapping) m;
					target.add(assemblerMapping.assembler, assemblerMapping.policy);
				} else if (m instanceof RelationMapping) {
					RelationMapping relationMapping = (RelationMapping) m;
					target.add((BeanRelationFixer<C, Object>) relationMapping.combiner::apply, relationMapping.relatedBeanCreator);
				}
			});
		}
		
		/**
		 * A marking interface for different kind of mapping configurations, made to allow them being stored in {@link java.util.Collection}.
		 * Only used by {@link QueryMapping}
		 */
		private interface Mapping {
			
		}
		
		/** Simple store for property-column mapping */
		private static class ColumnMapping implements Mapping {
			private final QueryMapper.ColumnMapping columnMapping;
			
			private ColumnMapping(QueryMapper.ColumnMapping columnMapping) {
				this.columnMapping = columnMapping;
			}
		}
		
		/** Simple store for assembly mapping */
		private static class AssemblerMapping implements Mapping {
			private final ResultSetRowAssembler assembler;
			private final AssemblyPolicy policy;
			
			private AssemblerMapping(ResultSetRowAssembler assembler, AssemblyPolicy policy) {
				this.assembler = assembler;
				this.policy = policy;
			}
		}
		
		/** Simple store for relation mapping */
		private static class RelationMapping implements Mapping {
			private final BeanRelationFixer combiner;
			private final ResultSetRowTransformer relatedBeanCreator;
			
			private RelationMapping(BeanRelationFixer combiner, ResultSetRowTransformer relatedBeanCreator) {
				this.combiner = combiner;
				this.relatedBeanCreator = relatedBeanCreator;
			}
		}
	}
}
