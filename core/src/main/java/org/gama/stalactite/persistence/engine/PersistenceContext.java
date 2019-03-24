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
import org.gama.lang.collection.ValueFactoryHashMap;
import org.gama.reflection.MethodReferenceCapturer;
import org.gama.sql.ConnectionProvider;
import org.gama.sql.dml.PreparedSQL;
import org.gama.sql.dml.WriteOperation;
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
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.query.builder.QueryBuilder;
import org.gama.stalactite.query.builder.SQLBuilder;
import org.gama.stalactite.query.model.CriteriaChain;
import org.gama.stalactite.query.model.Query;
import org.gama.stalactite.query.model.QueryEase;
import org.gama.stalactite.query.model.QueryProvider;

/**
 * Entry point for persistence in a database. Mix of configuration (Transaction, Dialect, ...) and registry for {@link Persister}s.
 *
 * @author Guillaume Mary
 */
public class PersistenceContext {
	
	private int jdbcBatchSize = 100;
	private final Map<Class<?>, Persister> persisterCache = new ValueFactoryHashMap<>(10, this::newPersister);
	
	private Dialect dialect;
	private ConnectionProvider connectionProvider;
	private final Map<Class, ClassMappingStrategy> mappingStrategies = new HashMap<>(50);
	
	public PersistenceContext() {
		
	}
	
	public PersistenceContext(ConnectionProvider connectionProvider, Dialect dialect) {
		this.connectionProvider = connectionProvider;
		this.dialect = dialect;
	}
	
	public ConnectionProvider getConnectionProvider() {
		return connectionProvider;
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
		Persister<C, I, T> persister = new Persister<>(this, classMappingStrategy);
		persisterCache.put(classMappingStrategy.getClassToPersist(), persister);
		return persister;
	}
	
	public Map<Class, ClassMappingStrategy> getMappingStrategies() {
		return mappingStrategies;
	}
	
	public Connection getCurrentConnection() {
		return connectionProvider.getCurrentConnection();
	}
	
	public Set<Persister> getPersisters() {
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
	public <C, I, T extends Table<T>> Persister<C, I, T> getPersister(Class<C> clazz) {
		return persisterCache.get(clazz);
	}
	
	public <C> void setPersister(Persister<C, ?, ?> persister) {
		persisterCache.put(persister.getMappingStrategy().getClassToPersist(), persister);
	}
	
	protected <C, I, T extends Table<T>> Persister<C, I, T> newPersister(Class<C> clazz) {
		ClassMappingStrategy<C, I, T> citClassMappingStrategy = ensureMappedClass(clazz);
		return new Persister<>(this, citClassMappingStrategy);
	}
	
	protected <C, I, T extends Table<T>> ClassMappingStrategy<C, I, T> ensureMappedClass(Class<C> clazz) {
		ClassMappingStrategy<C, I, T> mappingStrategy = getMappingStrategy(clazz);
		if (mappingStrategy == null) {
			throw new IllegalArgumentException("Unmapped entity " + clazz);
		} else {
			return mappingStrategy;
		}
	}
	
	public int getJDBCBatchSize() {
		return jdbcBatchSize;
	}
	
	public void setJDBCBatchSize(int jdbcBatchSize) {
		this.jdbcBatchSize = jdbcBatchSize;
	}
	
	/**
	 * Creates a {@link QueryConverter} from a {@link QueryProvider}, so it helps to build beans from a {@link Query}.
	 * Should be chained with {@link QueryConverter} mapping methods and obviously with its {@link QueryConverter#execute(ConnectionProvider)}
	 * method with {@link #getConnectionProvider()} as argument (for instance)
	 * 
	 * @param queryProvider the query provider to give the {@link Query} execute to populate beans
	 * @param beanType type of created beans, used for returned type marker
	 * @param <C> type of created beans
	 * @return a new {@link QueryConverter} that must be configured and executed
	 * @see org.gama.stalactite.query.model.QueryEase
	 */
	public <C> QueryConverter<C> newQuery(QueryProvider queryProvider, Class<C> beanType) {
		return newQuery(new QueryBuilder(queryProvider), beanType);
	}
	
	/**
	 * Creates a {@link QueryConverter} from a {@link Query} in order to build beans from the {@link Query}.
	 * Should be chained with {@link QueryConverter} mapping methods and obviously with its {@link QueryConverter#execute(ConnectionProvider)}
	 * method with {@link #getConnectionProvider()} as argument (for instance)
	 * 
	 * @param query the query to execute to populate beans
	 * @param beanType type of created beans, used for returned type marker
	 * @param <C> type of created beans
	 * @return a new {@link QueryConverter} that must be configured and executed
	 */
	public <C> QueryConverter<C> newQuery(Query query, Class<C> beanType) {
		return newQuery(new QueryBuilder(query), beanType);
	}
	
	/**
	 * Creates a {@link QueryConverter} from some SQL in order to build beans from the SQL.
	 * Should be chained with {@link QueryConverter} mapping methods and obviously with its {@link QueryConverter#execute(ConnectionProvider)}
	 * method with {@link #getConnectionProvider()} as argument (for instance)
	 * 
	 * @param sql the sql to execute to populate beans
	 * @param beanType type of created beans, used for returned type marker
	 * @param <C> type of created beans
	 * @return a new {@link QueryConverter} that must be configured and executed
	 */
	public <C> QueryConverter<C> newQuery(CharSequence sql, Class<C> beanType) {
		return newQuery(() -> sql, beanType);
	}
	
	public <C> QueryConverter<C> newQuery(SQLBuilder sql, Class<C> beanType) {
		return new QueryConverter<>(beanType, sql, getDialect().getColumnBinderRegistry());
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
		return execute(newQuery(QueryEase
				.select(column).from(column.getTable()), ((Class<C>) constructor.getDeclaringClass()))
				.mapKey(factory, column));
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
		return execute(newQuery(QueryEase
				.select(column1, column2).from(column1.getTable()), ((Class<C>) constructor.getDeclaringClass()))
				.mapKey(factory, column1, column2));
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
		Query query = QueryEase.select(column).from(column.getTable()).getSelectQuery();
		where.accept(query.getWhere());
		SelectMapping<C> selectMappingSupport = new SelectMapping<>();
		selectMapping.accept(selectMappingSupport);
		QueryConverter<C> queryConverter = newQuery(query, ((Class<C>) constructor.getDeclaringClass())).mapKey(factory, column);
		selectMappingSupport.appendTo(query, queryConverter);
		return execute(queryConverter);
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
		Query query = QueryEase.select(column1,column2).from(column1.getTable()).getSelectQuery();
		where.accept(query.getWhere());
		SelectMapping<C> selectMappingSupport = new SelectMapping<>();
		selectMapping.accept(selectMappingSupport);
		QueryConverter<C> queryConverter = newQuery(query, ((Class<C>) constructor.getDeclaringClass())).mapKey(factory, column1, column2);
		selectMappingSupport.appendTo(query, queryConverter);
		return execute(queryConverter);
	}
	
	public <C> List<C> execute(QueryConverter<C> queryProvider) {
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
		
		private void appendTo(Query query, QueryConverter<C> queryConverter) {
			mapping.keySet().forEach(query::select);
			mapping.forEach((k, v) -> queryConverter.map((Column) k, v));
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
			try (WriteOperation<Integer> writeOperation = new WriteOperation<>(updateStatement, connectionProvider)) {
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
		 * Executes this insert statement. To be used when insert has no parameters on where clause, else use {@link #execute(Map)}
		 *
		 * @return the inserted row count
		 */
		public int execute() {
			return execute(Collections.emptyMap());
		}
		
		/**
		 * Executes this insert statement.
		 *
		 * @return the inserted row count
		 */
		public int execute(Map<Column, Object> values) {
			InsertStatement<T> insertStatement = new InsertCommandBuilder<>(this).toStatement(getDialect().getColumnBinderRegistry());
			values.forEach(insertStatement::setValue);
			try (WriteOperation<Integer> writeOperation = new WriteOperation<>(insertStatement, connectionProvider)) {
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
			try (WriteOperation<Integer> writeOperation = new WriteOperation<>(deleteStatement, connectionProvider)) {
				writeOperation.setValues(deleteStatement.getValues());
				return writeOperation.execute();
			}
		}
	}
}
