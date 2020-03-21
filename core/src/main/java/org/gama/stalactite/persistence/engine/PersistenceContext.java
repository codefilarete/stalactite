package org.gama.stalactite.persistence.engine;

import java.lang.reflect.Constructor;
import java.sql.Connection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import org.danekja.java.util.function.serializable.SerializableBiConsumer;
import org.danekja.java.util.function.serializable.SerializableBiFunction;
import org.danekja.java.util.function.serializable.SerializableFunction;
import org.danekja.java.util.function.serializable.SerializableSupplier;
import org.gama.lang.Reflections;
import org.gama.lang.function.SerializableTriFunction;
import org.gama.lang.function.ThrowingConverter;
import org.gama.reflection.MethodReferenceCapturer;
import org.gama.reflection.MethodReferenceDispatcher;
import org.gama.stalactite.command.builder.DeleteCommandBuilder;
import org.gama.stalactite.command.builder.InsertCommandBuilder;
import org.gama.stalactite.command.builder.InsertCommandBuilder.InsertStatement;
import org.gama.stalactite.command.builder.UpdateCommandBuilder;
import org.gama.stalactite.command.builder.UpdateCommandBuilder.UpdateStatement;
import org.gama.stalactite.command.model.Delete;
import org.gama.stalactite.command.model.Insert;
import org.gama.stalactite.command.model.Update;
import org.gama.stalactite.persistence.mapping.ClassMappingStrategy;
import org.gama.stalactite.persistence.sql.Dialect;
import org.gama.stalactite.persistence.sql.IConnectionConfiguration;
import org.gama.stalactite.persistence.sql.IConnectionConfiguration.ConnectionConfigurationSupport;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.query.builder.SQLBuilder;
import org.gama.stalactite.query.builder.SQLQueryBuilder;
import org.gama.stalactite.query.model.CriteriaChain;
import org.gama.stalactite.query.model.Query;
import org.gama.stalactite.query.model.QueryEase;
import org.gama.stalactite.query.model.QueryProvider;
import org.gama.stalactite.sql.ConnectionProvider;
import org.gama.stalactite.sql.dml.PreparedSQL;
import org.gama.stalactite.sql.dml.WriteOperation;
import org.gama.stalactite.sql.result.ResultSetRowAssembler;

/**
 * Entry point for persistence in a database. Mix of configuration (Transaction, Dialect, ...) and registry for {@link Persister}s.
 *
 * @author Guillaume Mary
 */
public class PersistenceContext {
	
	private final Map<Class<?>, IEntityPersister> persisterCache = new HashMap<>();
	private final Dialect dialect;
	private final IConnectionConfiguration connectionConfiguration;
	private final Map<Class, ClassMappingStrategy> mappingStrategies = new HashMap<>(50);
	
	public PersistenceContext(ConnectionProvider connectionProvider, Dialect dialect) {
		this(new ConnectionConfigurationSupport(connectionProvider, 100), dialect);
	}
	
	public PersistenceContext(IConnectionConfiguration connectionConfiguration, Dialect dialect) {
		this.connectionConfiguration = connectionConfiguration;
		this.dialect = dialect;
	}
	
	public ConnectionProvider getConnectionProvider() {
		return connectionConfiguration.getConnectionProvider();
	}
	
	public Connection getCurrentConnection() {
		return getConnectionProvider().getCurrentConnection();
	}
	
	public int getJDBCBatchSize() {
		return getConnectionConfiguration().getBatchSize();
	}
	
	public Dialect getDialect() {
		return dialect;
	}
	
	public <C, I, T extends Table<T>> ClassMappingStrategy<C, I, T> getMappingStrategy(Class<C> aClass) {
		return mappingStrategies.get(aClass);
	}
	
	/**
	 * Add a persistence configuration to this instance
	 * 
	 * @param classMappingStrategy the persistence configuration
	 * @param <C> the entity type that is configured for persistence
	 * @param <I> the identifier type of the entity
	 * @return the newly created {@link Persister} for the configuration
	 */
	public <C, I, T extends Table<T>> Persister<C, I, T> add(ClassMappingStrategy<C, I, T> classMappingStrategy) {
		mappingStrategies.put(classMappingStrategy.getClassToPersist(), classMappingStrategy);
		Persister<C, I, T> persister = new Persister<>(classMappingStrategy, this);
		addPersister(persister);
		return persister;
	}
	
	public Map<Class, ClassMappingStrategy> getMappingStrategies() {
		return mappingStrategies;
	}
	
	public Set<IEntityPersister> getPersisters() {
		// copy the Set because values() is backed by the Map and getPersisters() is not expected to permit such modifications
		return new HashSet<>(persisterCache.values());
	}
	
	/**
	 * Returns the {@link Persister} mapped for a class.
	 * Prefer usage of that returned by {@link #add(ClassMappingStrategy)} because it's better typed (with identifier type)
	 * 
	 * @param clazz the class for which the {@link Persister} must be given
	 * @param <C> the type of the persisted entity
	 * @return never null
	 * @throws IllegalArgumentException if the class is not mapped
	 */
	public <C, I> IEntityPersister<C, I> getPersister(Class<C> clazz) {
		return persisterCache.get(clazz);
	}
	
	/**
	 * Registers a {@link Persister} on this instance. May overwrite an existing one
	 * 
	 * @param persister any {@link Persister}
	 * @param <C> type of persisted bean
	 */
	public <C> void addPersister(IEntityPersister<C, ?> persister) {
		IEntityPersister<C, ?> existingPersister = persisterCache.get(persister.getClassToPersist());
		if (existingPersister != null && existingPersister != persister) {
			throw new IllegalArgumentException("Persister already exists for class " + Reflections.toString(persister.getClassToPersist()));
		}
		
		persisterCache.put(persister.getClassToPersist(), persister);
	}
	
	public IConnectionConfiguration getConnectionConfiguration() {
		return this.connectionConfiguration;
	}
	
	/**
	 * Creates a {@link ExecutableSelect} from a {@link QueryProvider}, so it helps to build beans from a {@link Query}.
	 * Should be chained with {@link QueryMapper} mapping methods and obviously with its {@link ExecutableQuery#execute()}
	 * method with {@link #getConnectionProvider()} as argument (for instance)
	 * 
	 * @param queryProvider the query provider to give the {@link Query} execute to populate beans
	 * @param beanType type of created beans, used for returned type marker
	 * @param <C> type of created beans
	 * @return a new {@link ExecutableSelect} that must be configured and executed
	 * @see org.gama.stalactite.query.model.QueryEase
	 */
	public <C> ExecutableSelect<C> newQuery(QueryProvider queryProvider, Class<C> beanType) {
		return newQuery(new SQLQueryBuilder(queryProvider), beanType);
	}
	
	/**
	 * Creates a {@link ExecutableSelect} from a {@link Query} in order to build beans from the {@link Query}.
	 * Should be chained with {@link MappableQuery} mapping methods and obviously with its {@link ExecutableQuery#execute()}
	 * method with {@link #getConnectionProvider()} as argument (for instance)
	 * 
	 * @param query the query to execute to populate beans
	 * @param beanType type of created beans, used for returned type marker
	 * @param <C> type of created beans
	 * @return a new {@link ExecutableSelect} that must be configured and executed
	 */
	public <C> ExecutableSelect<C> newQuery(Query query, Class<C> beanType) {
		return newQuery(new SQLQueryBuilder(query), beanType);
	}
	
	/**
	 * Creates a {@link ExecutableSelect} from some SQL in order to build beans from the SQL.
	 * Should be chained with {@link ExecutableSelect} mapping methods and obviously with its {@link ExecutableQuery#execute()}
	 * method with {@link #getConnectionProvider()} as argument (for instance)
	 * 
	 * @param sql the sql to execute to populate beans
	 * @param beanType type of created beans, used for returned type marker
	 * @param <C> type of created beans
	 * @return a new {@link ExecutableSelect} that must be configured and executed
	 */
	public <C> ExecutableSelect<C> newQuery(CharSequence sql, Class<C> beanType) {
		return newQuery(() -> sql, beanType);
	}
	
	public <C> ExecutableSelect<C> newQuery(SQLBuilder sql, Class<C> beanType) {
		return wrapIntoExecutable(newConvertibleQuery(sql, beanType));
	}
	
	private <C> QueryMapper<C> newConvertibleQuery(SQLBuilder sql, Class<C> beanType) {
		return new QueryMapper<>(beanType, sql, getDialect().getColumnBinderRegistry());
	}
	
	/**
	 * Queries the database for the given column and fills some beans from it with the given constructor.
	 * 
	 * Usage is for very simple cases because IT DOESN'T FILTER DATABASE ROWS : no where clause is appended to the query.
	 * Moreover only column table is queried : no join nor assembly is made.
	 * Prefer {@link #select(SerializableFunction, Column, Consumer, Consumer)} for a more complete use case, or even {@link #newQuery(SQLBuilder, Class)}
	 * 
	 * @param factory a one-argument bean constructor
	 * @param column any table column (primary key may be prefered because its result is given to bean constructor but it is not expected)
	 * @param <C> bean type
	 * @param <I> constructor arg and column types
	 * @param <T> table type
	 * @return a list of all table records mapped to the given bean
	 * @see #select(SerializableFunction, Column, Consumer, Consumer)
	 * @see #newQuery(SQLBuilder, Class) 
	 */
	public <C, I, T extends Table> List<C> select(SerializableFunction<I, C> factory, Column<T, I> column) {
		Constructor constructor = new MethodReferenceCapturer().findConstructor(factory);
		return newQuery(QueryEase
				.select(column).from(column.getTable()), ((Class<C>) constructor.getDeclaringClass()))
				.mapKey(factory, column)
				.execute();
	}
	
	private <C> ExecutableSelect<C> wrapIntoExecutable(QueryMapper<C> queryMapperSupport) {
		MethodReferenceDispatcher methodDispatcher = new MethodReferenceDispatcher();
		SerializableFunction<ExecutableQuery, List<C>> execute = ExecutableQuery::execute;
		return methodDispatcher
				.redirect(execute, () -> execute(queryMapperSupport))
				.redirect(MappableQuery.class, queryMapperSupport, true)
				.build((Class<ExecutableSelect<C>>) (Class) ExecutableSelect.class);
	}
	
	/**
	 * Queries the database for the given columns and fills some beans from it with the given constructor.
	 *
	 * Usage is for very simple cases because IT DOESN'T FILTER DATABASE ROWS : no where clause is appended to the query.
	 * Moreover only columns table is queried : no join nor assembly are processed.
	 * Prefer {@link #select(SerializableBiFunction, Column, Column, Consumer, Consumer)} for a more complete use case, or even {@link #newQuery(SQLBuilder, Class)}
	 *
	 * @param factory a two-arguments bean constructor
	 * @param column1 a table column (may be a primary key column because its result is given to bean constructor but it is not expected)
	 * @param column2 a table column (may be a primary key column because its result is given to bean constructor but it is not expected)
	 * @param <C> bean type
	 * @param <I> constructor arg and column types
	 * @param <T> table type
	 * @return a list of all table records mapped to the given bean
	 * @see #select(SerializableBiFunction, Column, Column, Consumer, Consumer)
	 * @see #newQuery(SQLBuilder, Class)
	 */
	public <C, I, J, T extends Table> List<C> select(SerializableBiFunction<I, J, C> factory, Column<T, I> column1, Column<T, J> column2) {
		Constructor constructor = new MethodReferenceCapturer().findConstructor(factory);
		return newQuery(QueryEase.select(column1, column2).from(column1.getTable()), ((Class<C>) constructor.getDeclaringClass()))
				.mapKey(factory, column1, column2)
				.execute();
	}
	
	/**
	 * Queries the database for the given column and fills some beans from it with the given constructor.
	 *
	 * Usage is for very simple cases because IT DOESN'T FILTER DATABASE ROWS : no where clause is appended to the query.
	 * Moreover only column table is queried : no join nor assembly are processed.
	 * Prefer {@link #select(SerializableFunction, Column, Consumer, Consumer)} for a more complete use case, or even {@link #newQuery(SQLBuilder, Class)}
	 *
	 * @param factory a one-argument bean constructor
	 * @param column any table column (primary key may be prefered because its result is given to bean constructor but it is not expected)
	 * @param selectMapping allow to add some mapping beyond instanciation time
	 * @param <C> bean type
	 * @param <I> constructor arg and column types
	 * @param <T> table type
	 * @return a list of all table records mapped to the given bean
	 * @see #select(SerializableFunction, Column, Consumer, Consumer)
	 * @see #newQuery(SQLBuilder, Class)
	 */
	public <C, I, T extends Table> List<C> select(SerializableFunction<I, C> factory, Column<T, I> column, Consumer<SelectMapping<C>> selectMapping) {
		return select(factory, column, selectMapping, where -> {});
	}
	
	/**
	 * Queries the database for the given column and fills some beans from it with the given constructor.
	 *
	 * Usage is for very simple cases because IT DOESN'T FILTER DATABASE ROWS : no where clause is appended to the query.
	 * Moreover only columns table is queried : no join nor assembly are processed.
	 * Prefer {@link #select(SerializableFunction, Column, Consumer, Consumer)} for a more complete use case, or even {@link #newQuery(SQLBuilder, Class)}
	 *
	 * @param factory a two-arguments bean constructor
	 * @param column1 a table column (may be a primary key column because its result is given to bean constructor but it is not expected)
	 * @param column2 a table column (may be a primary key column because its result is given to bean constructor but it is not expected)
	 * @param selectMapping allow to add some mapping beyond instanciation time
	 * @param <C> bean type
	 * @param <I> constructor arg and column types
	 * @param <T> table type
	 * @return a list of all table records mapped to the given bean
	 * @see #select(SerializableFunction, Column, Consumer, Consumer)
	 * @see #newQuery(SQLBuilder, Class)
	 */
	public <C, I, J, T extends Table> List<C> select(SerializableBiFunction<I, J, C> factory, Column<T, I> column1, Column<T, J> column2,
													 Consumer<SelectMapping<C>> selectMapping) {
		return select(factory, column1, column2, selectMapping, where -> {});
	}
	
	/**
	 * Queries the database for the given column and fills some beans from it with the given constructor.
	 *
	 * Usage is for very simple cases : only columns table is targeted (no join nor assembly are processed).
	 * Prefer {@link #newQuery(SQLBuilder, Class)} for a more complete use case.
	 *
	 * @param factory a one-argument bean constructor
	 * @param column any table column (primary key may be prefered because its result is given to bean constructor but it is not expected)
	 * @param selectMapping allow to add some mapping beyond instanciation time
	 * @param <C> bean type
	 * @param <I> constructor arg and column types
	 * @param <T> table type
	 * @return a list of all table records mapped to the given bean
	 * @see #newQuery(SQLBuilder, Class)
	 */
	public <C, I, T extends Table> List<C> select(SerializableFunction<I, C> factory, Column<T, I> column,
												  Consumer<SelectMapping<C>> selectMapping,
												  Consumer<CriteriaChain> where) {
		Constructor constructor = new MethodReferenceCapturer().findConstructor(factory);
		Query query = QueryEase.select(column).from(column.getTable()).getQuery();
		where.accept(query.getWhere());
		SelectMapping<C> selectMappingSupport = new SelectMapping<>();
		selectMapping.accept(selectMappingSupport);
		QueryMapper<C> queryMapper = newConvertibleQuery(new SQLQueryBuilder(query), ((Class<C>) constructor.getDeclaringClass()));
		queryMapper.mapKey(factory, column);
		selectMappingSupport.appendTo(query, queryMapper);
		return execute(queryMapper);
	}
	
	/**
	 * Queries the database for the given column and fills some beans from it with the given constructor.
	 *
	 * Usage is for very simple cases : only columns table is targeted (no join nor assembly are processed).
	 * Prefer {@link #newQuery(SQLBuilder, Class)} for a more complete use case.
	 *
	 * @param factory a one-argument bean constructor
	 * @param column1 a table column (may be a primary key column because its result is given to bean constructor but it is not expected)
	 * @param column2 a table column (may be a primary key column because its result is given to bean constructor but it is not expected)
	 * @param selectMapping allow to add some mapping beyond instanciation time
	 * @param <C> bean type
	 * @param <I> constructor arg and column types
	 * @param <T> table type
	 * @return a list of all table records mapped to the given bean
	 * @see #newQuery(SQLBuilder, Class)
	 */
	public <C, I, J, T extends Table> List<C> select(SerializableBiFunction<I, J, C> factory, Column<T, I> column1, Column<T, J> column2,
												  Consumer<SelectMapping<C>> selectMapping,
												  Consumer<CriteriaChain> where) {
		Constructor constructor = new MethodReferenceCapturer().findConstructor(factory);
		Query query = QueryEase.select(column1,column2).from(column1.getTable()).getQuery();
		where.accept(query.getWhere());
		SelectMapping<C> selectMappingSupport = new SelectMapping<>();
		selectMapping.accept(selectMappingSupport);
		QueryMapper<C> queryMapper = newConvertibleQuery(new SQLQueryBuilder(query), ((Class<C>) constructor.getDeclaringClass()));
		queryMapper.mapKey(factory, column1, column2);
		selectMappingSupport.appendTo(query, queryMapper);
		return execute(queryMapper);
	}
	
	public <C> List<C> execute(QueryMapper<C> queryProvider) {
		return queryProvider.execute(getConnectionProvider());
	}
	
	public <T extends Table> ExecutableUpdate<T> update(T table) {
		return new ExecutableUpdate<>(table);
	}
	
	public <T extends Table> ExecutableInsert<T> insert(T table) {
		return new ExecutableInsert<>(table);
	}
	
	public <T extends Table> ExecutableDelete<T> delete(T table) {
		return new ExecutableDelete<>(table);
	}
	
	/**
	 * Small support to store additional mapping of select queries.
	 * 
	 * @param <C> resulting bean type
	 */
	public static class SelectMapping<C> {
		
		private final Map<Column<Table, ?>, SerializableBiConsumer<C, ?>> mapping = new HashMap<>();
		
		public <T extends Table, O> SelectMapping<C> add(Column<T, O> c, SerializableBiConsumer<C, O> setter) {
			mapping.put((Column) c, setter);
			return this;
		}
		
		private void appendTo(Query query, QueryMapper<C> queryMapper) {
			mapping.keySet().forEach(query::select);
			mapping.forEach((k, v) -> queryMapper.map((Column) k, v));
		}
	}
	
	public class ExecutableUpdate<T extends Table> extends Update<T> {
		
		private ExecutableUpdate(T targetTable) {
			super(targetTable);
		}
		
		/** Overriden to adapt return type */
		@Override
		public ExecutableUpdate<T> set(Column column) {
			super.set(column);
			return this;
		}
		
		/** Overriden to adapt return type */
		@Override
		public <C> ExecutableUpdate<T> set(Column<T, C> column, C value) {
			super.set(column, value);
			return this;
		}
		
		/** Overriden to adapt return type */
		@Override
		public <C> ExecutableUpdate<T> set(Column<T, C> column1, Column<T, C> column2) {
			super.set(column1, column2);
			return this;
		}
		
		/**
		 * Executes this update statement. To be used when update has no parameters on where clause, else use {@link #execute(Map)}
		 * 
		 * @return the updated row count
		 */
		public int execute() {
			return execute(Collections.emptyMap());
		}
		
		/**
		 * Executes this update statement with given values.
		 *
		 * @return the updated row count
		 */
		public int execute(Map<? extends Column<T, Object>, Object> values) {
			UpdateStatement<T> updateStatement = new UpdateCommandBuilder<>(this).toStatement(getDialect().getColumnBinderRegistry());
			values.forEach(updateStatement::setValue);
			try (WriteOperation<Integer> writeOperation = new WriteOperation<>(updateStatement, getConnectionProvider())) {
				writeOperation.setValues(updateStatement.getValues());
				return writeOperation.execute();
			}
		}
	}
	
	public class ExecutableInsert<T extends Table> extends Insert<T> {
		
		private ExecutableInsert(T table) {
			super(table);
		}
		
		/** Overriden to adapt return type */
		@Override
		public <C> ExecutableInsert<T> set(Column<T, C> column, C value) {
			super.set(column, value);
			return this;
		}
		
		/**
		 * Executes this insert statement.
		 *
		 * @return the inserted row count
		 */
		public int execute() {
			InsertStatement<T> insertStatement = new InsertCommandBuilder<>(this).toStatement(getDialect().getColumnBinderRegistry());
			try (WriteOperation<Integer> writeOperation = new WriteOperation<>(insertStatement, getConnectionProvider())) {
				writeOperation.setValues(insertStatement.getValues());
				return writeOperation.execute();
			}
		}
	}
	
	public class ExecutableDelete<T extends Table> extends Delete<T> {
		
		private ExecutableDelete(T table) {
			super(table);
		}
		
		/**
		 * Executes this delete statement with given values.
		 *
		 * @return the deleted row count
		 */
		public int execute() {
			PreparedSQL deleteStatement = new DeleteCommandBuilder<T>(this).toStatement(getDialect().getColumnBinderRegistry());
			try (WriteOperation<Integer> writeOperation = new WriteOperation<>(deleteStatement, getConnectionProvider())) {
				writeOperation.setValues(deleteStatement.getValues());
				return writeOperation.execute();
			}
		}
	}
	
	/**
	 * Mashup between {@link MappableQuery} and {@link ExecutableQuery} to make an {@link MappableQuery} executable
	 * @param <C> type of object returned by query execution
	 */
	public interface ExecutableSelect<C> extends MappableQuery<C>, ExecutableQuery<C> {
		
		@Override
		<I> ExecutableSelect<C> mapKey(SerializableFunction<I, C> factory, String columnName, Class<I> columnType);
		
		@Override
		<I, J> ExecutableSelect<C> mapKey(SerializableBiFunction<I, J, C> factory, String column1Name, Class<I> column1Type,
										  String column2Name, Class<J> column2Type);
		
		@Override
		<I, J, K> ExecutableSelect<C> mapKey(SerializableTriFunction<I, J, K, C> factory, String column1Name, Class<I> column1Type,
											 String column2Name, Class<J> column2Type,
											 String column3Name, Class<K> column3Type);
		
		@Override
		<I> ExecutableSelect<C> mapKey(SerializableFunction<I, C> factory, Column<? extends Table, I> column);
		
		@Override
		<I, J> ExecutableSelect<C> mapKey(SerializableBiFunction<I, J, C> factory, Column<? extends Table, I> column1, Column<? extends Table, J> column2);
		
		@Override
		<I, J, K> ExecutableSelect<C> mapKey(SerializableTriFunction<I, J, K, C> factory,
											 Column<? extends Table, I> column1,
											 Column<? extends Table, J> column2,
											 Column<? extends Table, K> column3
		);
		
		@Override
		<I> ExecutableSelect<C> mapKey(SerializableSupplier<C> javaBeanCtor, String columnName, SerializableBiConsumer<C, I> keySetter);
		
		@Override
		<I> ExecutableSelect<C> mapKey(SerializableSupplier<C> javaBeanCtor, Column<? extends Table, I> column, SerializableBiConsumer<C, I> keySetter);
		
		@Override
		<I> ExecutableSelect<C> map(String columnName, SerializableBiConsumer<C, I> setter, Class<I> columnType);
		
		@Override
		<I, J, E extends RuntimeException> ExecutableSelect<C> map(String columnName, SerializableBiConsumer<C, J> setter, Class<I> columnType, ThrowingConverter<I, J, E> converter);
		
		@Override
		<I> ExecutableSelect<C> map(String columnName, SerializableBiConsumer<C, I> setter);
		
		@Override
		<I, E extends RuntimeException> ExecutableSelect<C> map(String columnName, SerializableBiConsumer<C, I> setter, ThrowingConverter<I, I, E> converter);
		
		@Override
		<I> ExecutableSelect<C> map(Column<? extends Table, I> column, SerializableBiConsumer<C, I> setter);
		
		@Override
		<I, J, E extends RuntimeException> ExecutableSelect<C> map(Column<? extends Table, I> column, SerializableBiConsumer<C, J> setter, ThrowingConverter<I, J, E> converter);
		
		@Override
		ExecutableSelect<C> add(ResultSetRowAssembler<C> assembler);
		
		@Override
		ExecutableSelect<C> set(String paramName, Object value);
		
		@Override
		<O> ExecutableSelect<C> set(String paramName, O value, Class<? super O> valueType);
		
		@Override
		<O> ExecutableSelect<C> set(String paramName, Iterable<O> value, Class<? super O> valueType);
	}
}
