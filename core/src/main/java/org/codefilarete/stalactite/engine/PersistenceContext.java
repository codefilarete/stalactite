package org.codefilarete.stalactite.engine;

import javax.sql.DataSource;
import java.lang.reflect.Constructor;
import java.lang.reflect.Executable;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import org.codefilarete.reflection.MethodReferenceCapturer;
import org.codefilarete.reflection.MethodReferenceDispatcher;
import org.codefilarete.stalactite.engine.runtime.Persister;
import org.codefilarete.stalactite.mapping.ClassMapping;
import org.codefilarete.stalactite.query.builder.QuerySQLBuilder;
import org.codefilarete.stalactite.query.builder.SQLBuilder;
import org.codefilarete.stalactite.query.model.AbstractRelationalOperator;
import org.codefilarete.stalactite.query.model.CriteriaChain;
import org.codefilarete.stalactite.query.model.Query;
import org.codefilarete.stalactite.query.model.QueryEase;
import org.codefilarete.stalactite.query.model.QueryProvider;
import org.codefilarete.stalactite.sql.ConnectionConfiguration;
import org.codefilarete.stalactite.sql.ConnectionConfiguration.ConnectionConfigurationSupport;
import org.codefilarete.stalactite.sql.ConnectionProvider;
import org.codefilarete.stalactite.sql.CurrentThreadConnectionProvider;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.DialectResolver;
import org.codefilarete.stalactite.sql.ServiceLoaderDialectResolver;
import org.codefilarete.stalactite.sql.TransactionAwareConnectionProvider;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.order.Delete;
import org.codefilarete.stalactite.sql.order.DeleteCommandBuilder;
import org.codefilarete.stalactite.sql.order.Insert;
import org.codefilarete.stalactite.sql.order.InsertCommandBuilder;
import org.codefilarete.stalactite.sql.order.InsertCommandBuilder.InsertStatement;
import org.codefilarete.stalactite.sql.order.Update;
import org.codefilarete.stalactite.sql.order.UpdateCommandBuilder;
import org.codefilarete.stalactite.sql.order.UpdateCommandBuilder.UpdateStatement;
import org.codefilarete.stalactite.sql.result.BeanRelationFixer;
import org.codefilarete.stalactite.sql.result.ResultSetRowAssembler;
import org.codefilarete.stalactite.sql.result.ResultSetRowTransformer;
import org.codefilarete.stalactite.sql.result.WholeResultSetTransformer.AssemblyPolicy;
import org.codefilarete.stalactite.sql.statement.PreparedSQL;
import org.codefilarete.stalactite.sql.statement.WriteOperation;
import org.codefilarete.tool.Nullable;
import org.codefilarete.tool.Reflections;
import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.tool.exception.Exceptions;
import org.codefilarete.tool.function.Converter;
import org.codefilarete.tool.function.SerializableTriFunction;
import org.danekja.java.util.function.serializable.SerializableBiConsumer;
import org.danekja.java.util.function.serializable.SerializableBiFunction;
import org.danekja.java.util.function.serializable.SerializableFunction;
import org.danekja.java.util.function.serializable.SerializableSupplier;

/**
 * Entry point for persistence in a database. Mix of configuration (Transaction, Dialect, ...) and registry for {@link Persister}s.
 *
 * @author Guillaume Mary
 * @see #PersistenceContext(DataSource)
 */
public class PersistenceContext implements PersisterRegistry {
	
	private final Map<Class<?>, EntityPersister> persisterCache = new HashMap<>();
	private final Dialect dialect;
	private final TransactionAwareConnectionConfiguration connectionConfiguration;
	private final Map<Class, ClassMapping> mapping = new HashMap<>(50);
	
	/**
	 * Constructor with minimal but necessary information.
	 * JDBC batch size is set to 100.
	 * Dialect is deduced from JVM Service Provider and connection metadata.
	 * 
	 * @param dataSource a JDBC {@link Connection} provider
	 */
	public PersistenceContext(DataSource dataSource) {
		this(new CurrentThreadConnectionProvider(dataSource));
	}
	
	/**
	 * Constructor with minimal but necessary information.
	 * JDBC batch size is set to 100.
	 *
	 * @param dataSource a JDBC {@link Connection} provider
	 * @param dialect dialect to be used with {@link Connection} given by dataSource
	 */
	public PersistenceContext(DataSource dataSource, Dialect dialect) {
		this(new CurrentThreadConnectionProvider(dataSource), dialect);
	}
	
	/**
	 * Constructor with minimal but necessary information.
	 * JDBC batch size is set to 100.
	 * Dialect is deduced from JVM Service Provider and connection metadata.
	 *
	 * @param connectionProvider a JDBC {@link Connection} provider
	 */
	public PersistenceContext(ConnectionProvider connectionProvider) {
		this(new ConnectionConfigurationSupport(connectionProvider, 100), new ServiceLoaderDialectResolver());
	}
	
	/**
	 * Constructor with {@link Connection} information.
	 * Dialect is deduced from JVM Service Provider and connection metadata.
	 *
	 * @param connectionConfiguration necessary information on JDBC {@link Connection}
	 */
	public PersistenceContext(ConnectionConfiguration connectionConfiguration) {
		this(connectionConfiguration, new ServiceLoaderDialectResolver());
	}
	
	/**
	 * Constructor with {@link Connection} provider and dialect provider.
	 * JDBC batch size is set to 100.
	 * 
	 * @param connectionProvider a JDBC {@link Connection} provider
	 * @param dialectResolver dialect provider
	 */
	public PersistenceContext(ConnectionProvider connectionProvider, DialectResolver dialectResolver) {
		this(new ConnectionConfigurationSupport(connectionProvider, 100), dialectResolver);
	}
	
	/**
	 * Constructor with {@link Connection} information and dialect provider.
	 *
	 * @param connectionConfiguration necessary information on JDBC {@link Connection}
	 * @param dialectResolver dialect provider
	 */
	public PersistenceContext(ConnectionConfiguration connectionConfiguration, DialectResolver dialectResolver) {
		this.connectionConfiguration = new TransactionAwareConnectionConfiguration(connectionConfiguration);
		try (Connection currentConnection = this.connectionConfiguration.giveConnection()) {
			this.dialect = dialectResolver.determineDialect(currentConnection);
		} catch (SQLException throwable) {
			throw Exceptions.asRuntimeException(throwable);
		}
	}
	
	/**
	 * Constructor with {@link Connection} provider and dialect.
	 * JDBC batch size is set to 100.
	 *
	 * @param connectionProvider a JDBC {@link Connection} provider
	 * @param dialect dialect to be used with {@link Connection}
	 */
	public PersistenceContext(ConnectionProvider connectionProvider, Dialect dialect) {
		this(new ConnectionConfigurationSupport(connectionProvider, 100), dialect);
	}
	
	/**
	 * Constructor with {@link Connection} information and dialect.
	 *
	 * @param connectionConfiguration necessary information on JDBC {@link Connection}
	 * @param dialect dialect to be used with {@link Connection}
	 */
	public PersistenceContext(ConnectionConfiguration connectionConfiguration, Dialect dialect) {
		this.connectionConfiguration = new TransactionAwareConnectionConfiguration(connectionConfiguration);
		this.dialect = dialect;
	}
	
	public ConnectionProvider getConnectionProvider() {
		return connectionConfiguration.getConnectionProvider();
	}
	
	public int getJDBCBatchSize() {
		return getConnectionConfiguration().getBatchSize();
	}
	
	public Dialect getDialect() {
		return dialect;
	}
	
	public <C, I, T extends Table<T>> ClassMapping<C, I, T> getMapping(Class<C> aClass) {
		return mapping.get(aClass);
	}
	
	/**
	 * Adds a persistence configuration to this instance
	 * 
	 * @param classMappingStrategy the persistence configuration
	 * @param <C> the entity type that is configured for persistence
	 * @param <I> the identifier type of the entity
	 * @return the newly created {@link Persister} for the configuration
	 */
	public <C, I, T extends Table<T>> Persister<C, I, T> add(ClassMapping<C, I, T> classMappingStrategy) {
		mapping.put(classMappingStrategy.getClassToPersist(), classMappingStrategy);
		Persister<C, I, T> persister = new Persister<>(classMappingStrategy, this);
		addPersister(persister);
		return persister;
	}
	
	public Set<EntityPersister> getPersisters() {
		// copy the Set because values() is backed by the Map and getPersisters() is not expected to permit such modifications
		return new HashSet<>(persisterCache.values());
	}
	
	/**
	 * Returns the {@link Persister} mapped for a class.
	 * 
	 * @param clazz the class for which the {@link Persister} must be given
	 * @param <C> the type of the persisted entity
	 * @return null if class has no persister registered
	 */
	public <C, I> EntityPersister<C, I> getPersister(Class<C> clazz) {
		return persisterCache.get(clazz);
	}
	
	/**
	 * Registers a {@link Persister} on this instance. May overwrite an existing one
	 * 
	 * @param persister any {@link Persister}
	 * @param <C> type of persisted bean
	 * @throws IllegalArgumentException if a persister already exists for class persisted by given persister
	 */
	public <C> void addPersister(EntityPersister<C, ?> persister) {
		EntityPersister<C, ?> existingPersister = persisterCache.get(persister.getClassToPersist());
		if (existingPersister != null && existingPersister != persister) {
			throw new IllegalArgumentException("Persister already exists for class " + Reflections.toString(persister.getClassToPersist()));
		}
		
		persisterCache.put(persister.getClassToPersist(), persister);
	}
	
	public ConnectionConfiguration getConnectionConfiguration() {
		return this.connectionConfiguration;
	}
	
	/**
	 * Creates a {@link ExecutableBeanPropertyKeyQueryMapper} from a {@link QueryProvider}, so it helps to build beans from a {@link Query}.
	 * Should be chained with {@link QueryMapper} mapping methods and obviously with its {@link ExecutableQuery#execute()}
	 * 
	 * @param queryProvider the query provider to give the {@link Query} execute to populate beans
	 * @param beanType type of created beans, used for returned type marker
	 * @param <C> type of created beans
	 * @return a new {@link ExecutableBeanPropertyKeyQueryMapper} that must be configured and executed
	 * @see org.codefilarete.stalactite.query.model.QueryEase
	 */
	public <C> ExecutableBeanPropertyKeyQueryMapper<C> newQuery(QueryProvider<Query> queryProvider, Class<C> beanType) {
		return newQuery(new QuerySQLBuilder(queryProvider), beanType);
	}
	
	/**
	 * Creates a {@link ExecutableBeanPropertyKeyQueryMapper} from a {@link Query} in order to build beans from the {@link Query}.
	 * Should be chained with {@link ExecutableBeanPropertyKeyQueryMapper} mapping methods and obviously with its {@link ExecutableQuery#execute()}
	 * 
	 * @param query the query to execute to populate beans
	 * @param beanType type of created beans, used for returned type marker
	 * @param <C> type of created beans
	 * @return a new {@link ExecutableBeanPropertyKeyQueryMapper} that must be configured and executed
	 */
	public <C> ExecutableBeanPropertyKeyQueryMapper<C> newQuery(Query query, Class<C> beanType) {
		return newQuery(new QuerySQLBuilder(query), beanType);
	}
	
	/**
	 * Creates a {@link ExecutableBeanPropertyKeyQueryMapper} from some SQL in order to build beans from the SQL.
	 * Should be chained with {@link ExecutableBeanPropertyKeyQueryMapper} mapping methods and obviously with its {@link ExecutableQuery#execute()}
	 * 
	 * @param sql the SQL to execute to populate beans
	 * @param beanType type of created beans, used for returned type marker
	 * @param <C> type of created beans
	 * @return a new {@link ExecutableBeanPropertyKeyQueryMapper} that must be configured and executed
	 */
	public <C> ExecutableBeanPropertyKeyQueryMapper<C> newQuery(CharSequence sql, Class<C> beanType) {
		return newQuery(() -> sql, beanType);
	}
	
	/**
	 * Same as {@link #newQuery(CharSequence, Class)} with an {@link SQLBuilder} as argument to be more flexible : final SQL will be built just
	 * before execution.
	 * 
	 * @param sql the builder of SQL to be called for final SQL
	 * @param beanType type of created beans, used for returned type marker
	 * @param <C> type of created beans
	 * @return a new {@link ExecutableBeanPropertyKeyQueryMapper} that must be configured and executed
	 */
	public <C> ExecutableBeanPropertyKeyQueryMapper<C> newQuery(SQLBuilder sql, Class<C> beanType) {
		return wrapIntoExecutable(newTransformableQuery(sql, beanType));
	}
	
	private <C> ExecutableBeanPropertyKeyQueryMapper<C> wrapIntoExecutable(QueryMapper<C> queryMapperSupport) {
		SingleResultExecutableSelect<C> singleResultDispatcher = new MethodReferenceDispatcher()
				.redirect(SingleResultExecutableSelect<C>::execute, () -> executeUnique(queryMapperSupport))
				.redirect(BeanPropertyQueryMapper.class, queryMapperSupport, true)
				.build((Class<SingleResultExecutableSelect<C>>) (Class) SingleResultExecutableSelect.class);
		
		ExecutableBeanPropertyQueryMapper<C> beanPropertyMappingHandler = new MethodReferenceDispatcher()
				// since ExecutableBeanPropertyQueryMapper can be run we redirect execute() method to the underlying method
				.redirect(ExecutableQuery<C>::execute, () -> execute(queryMapperSupport))
				// redirecting singleResult() to convenient instance handler
				.redirect(ExecutableBeanPropertyQueryMapper<C>::singleResult, () -> singleResultDispatcher)
				.redirect(BeanPropertyQueryMapper.class, queryMapperSupport, true)
				.build((Class<ExecutableBeanPropertyQueryMapper<C>>) (Class) ExecutableBeanPropertyQueryMapper.class);
		
		return new MethodReferenceDispatcher()
				// BeanKeyQueryMapper methods are applied to a convenient instance but their results are redirected to another one to comply with their signature 
				.redirect(BeanKeyQueryMapper.class, queryMapperSupport, beanPropertyMappingHandler)
				// since ExecutableBeanPropertyKeyQueryMapper can be run we redirect execute() method to the underlying method
				.redirect(ExecutableQuery<C>::execute, () -> execute(queryMapperSupport))
				.build((Class<ExecutableBeanPropertyKeyQueryMapper<C>>) (Class) ExecutableBeanPropertyKeyQueryMapper.class);
	}
	
	private <C> QueryMapper<C> newTransformableQuery(SQLBuilder sql, Class<C> beanType) {
		return new QueryMapper<>(beanType, sql, getDialect().getColumnBinderRegistry());
	}
	
	/**
	 * Queries the database for given column and invokes given 1-arg constructor with it.
	 * 
	 * Usage is for very simple cases because IT DOESN'T FILTER DATABASE ROWS : no where clause is appended to the query.
	 * Moreover, only column table is queried : no join nor assembly is made.
	 * Prefer {@link #select(SerializableFunction, Column, Consumer, Consumer)} for a more complete use case, or even {@link #newQuery(SQLBuilder, Class)}
	 * 
	 * @param factory a one-argument bean constructor
	 * @param column any table column (primary key may be prefered because its result is given to bean constructor but it is not expected)
	 * @param <C> type of created beans
	 * @param <I> constructor arg and column types
	 * @param <T> targeted table type
	 * @return a list of all table records mapped to the given bean
	 * @see #select(SerializableFunction, Column, Consumer, Consumer)
	 * @see #newQuery(SQLBuilder, Class) 
	 */
	public <C, I, T extends Table> List<C> select(SerializableFunction<I, C> factory, Column<T, I> column) {
		Executable constructor = new MethodReferenceCapturer().findExecutable(factory);
		return newQuery(QueryEase
				.select(column).from(column.getTable()), ((Class<C>) constructor.getDeclaringClass()))
				.mapKey(factory, column)
				.execute();
	}
	
	/**
	 * Queries the database for given columns and invokes given 2-args constructor with them. 
	 *
	 * Usage is for very simple cases because IT DOESN'T FILTER DATABASE ROWS : no where clause is appended to the query.
	 * Moreover, only columns table is queried : no join nor assembly are processed.
	 * Prefer {@link #select(SerializableBiFunction, Column, Column, Consumer, Consumer)} for a more complete use case, or even {@link #newQuery(SQLBuilder, Class)}
	 *
	 * @param factory a two-arguments bean constructor
	 * @param column1 a table column (may be a primary key column because its result is given to bean constructor but it is not expected)
	 * @param column2 a table column (may be a primary key column because its result is given to bean constructor but it is not expected)
	 * @param <C> type of created beans
	 * @param <I> constructor first-arg type and first column type
	 * @param <J> constructor second-arg type and second column type
	 * @param <T> targeted table type
	 * @return a list of all table records mapped to the given bean
	 * @see #select(SerializableBiFunction, Column, Column, Consumer, Consumer)
	 * @see #newQuery(SQLBuilder, Class)
	 */
	public <C, I, J, T extends Table> List<C> select(SerializableBiFunction<I, J, C> factory, Column<T, I> column1, Column<T, J> column2) {
		Constructor<C> constructor = new MethodReferenceCapturer().findConstructor(factory);
		return newQuery(QueryEase.select(column1, column2).from(column1.getTable()), constructor.getDeclaringClass())
				.mapKey(factory, column1, column2)
				.execute();
	}
	
	/**
	 * Queries the database for given columns and invokes given 3-args constructor with them.
	 *
	 * Usage is for very simple cases because IT DOESN'T FILTER DATABASE ROWS : no where clause is appended to the query.
	 * Moreover, only columns table is queried : no join nor assembly are processed.
	 * Prefer {@link #select(SerializableBiFunction, Column, Column, Consumer, Consumer)} for a more complete use case, or even {@link #newQuery(SQLBuilder, Class)}
	 *
	 * @param factory a two-arguments bean constructor
	 * @param column1 a table column (may be a primary key column because its result is given to bean constructor but it is not expected)
	 * @param column2 a table column (may be a primary key column because its result is given to bean constructor but it is not expected)
	 * @param column3 a table column (may be a primary key column because its result is given to bean constructor but it is not expected)
	 * @param <C> type of created beans
	 * @param <I> constructor first-arg type and first column type
	 * @param <J> constructor second-arg type and second column type
	 * @param <K> constructor third-arg type and third column type
	 * @param <T> targeted table type
	 * @return a list of all table records mapped to the given bean
	 * @see #select(SerializableBiFunction, Column, Column, Consumer, Consumer)
	 * @see #newQuery(SQLBuilder, Class)
	 */
	public <C, I, J, K, T extends Table> List<C> select(SerializableTriFunction<I, J, K, C> factory, Column<T, I> column1, Column<T, J> column2, Column<T, K> column3) {
		Constructor<C> constructor = new MethodReferenceCapturer().findConstructor(factory);
		return newQuery(QueryEase.select(column1, column2, column3).from(column1.getTable()), constructor.getDeclaringClass())
				.mapKey(factory, column1, column2, column3)
				.execute();
	}
	
	/**
	 * Queries the database and invokes given no-arg constructor for each row.
	 * Additional bean fulfillment will be done by using configuration you'll give through {@link SelectMapping}.
	 *
	 * Usage is for very simple cases because IT DOESN'T FILTER DATABASE ROWS : no where clause is appended to the query.
	 * Moreover, only columns table is queried : no join nor assembly are processed.
	 * Prefer {@link #select(SerializableBiFunction, Column, Column, Consumer, Consumer)} for a more complete use case, or even {@link #newQuery(SQLBuilder, Class)}
	 *
	 * @param factory a two-arguments bean constructor
	 * @param <C> type of created beans
	 * @param <T> targeted table type
	 * @return a list of all table records mapped to the given bean
	 * @see #select(SerializableBiFunction, Column, Column, Consumer, Consumer)
	 * @see #newQuery(SQLBuilder, Class)
	 */
	public <C, T extends Table> List<C> select(SerializableSupplier<C> factory, Consumer<SelectMapping<C, T>> selectMapping) {
		return select(factory, selectMapping, where -> {});
	}
	
	/**
	 * Queries the database for given columns and invokes given 1-arg constructor with it.
	 * Additional bean fulfillment will be done by using configuration you'll give through {@link SelectMapping}. 
	 *
	 * Usage is for very simple cases because IT DOESN'T FILTER DATABASE ROWS : no where clause is appended to the query.
	 * Moreover, only column table is queried : no join nor assembly are processed.
	 * Prefer {@link #select(SerializableFunction, Column, Consumer, Consumer)} for a more complete use case, or even {@link #newQuery(SQLBuilder, Class)}
	 *
	 * @param factory a one-argument bean constructor
	 * @param column any table column (primary key may be preferred because its result is given to bean constructor but it is not expected)
	 * @param selectMapping allow to add some mapping beyond instantiation time
	 * @param <C> type of created beans
	 * @param <I> constructor arg and column types
	 * @param <T> targeted table type
	 * @return a list of all table records mapped to the given bean
	 * @see #select(SerializableFunction, Column, Consumer, Consumer)
	 * @see #newQuery(SQLBuilder, Class)
	 */
	public <C, I, T extends Table> List<C> select(SerializableFunction<I, C> factory, Column<T, I> column, Consumer<SelectMapping<C, T>> selectMapping) {
		return select(factory, column, selectMapping, where -> {});
	}
	
	/**
	 * Queries the database for given columns and invokes given 2-args constructor with them.
	 * Additional bean fulfillment will be done by using configuration you'll give through {@link SelectMapping}.
	 *
	 * Usage is for very simple cases because IT DOESN'T FILTER DATABASE ROWS : no where clause is appended to the query.
	 * Moreover, only columns table is queried : no join nor assembly are processed.
	 * Prefer {@link #select(SerializableFunction, Column, Consumer, Consumer)} for a more complete use case, or even {@link #newQuery(SQLBuilder, Class)}
	 *
	 * @param factory a two-arguments bean constructor
	 * @param column1 a table column (may be a primary key column because its result is given to bean constructor but it is not expected)
	 * @param column2 a table column (may be a primary key column because its result is given to bean constructor but it is not expected)
	 * @param selectMapping allow to add some mapping beyond instantiation time
	 * @param <C> type of created beans
	 * @param <I> constructor arg and column types
	 * @param <T> targeted table type
	 * @return a list of all table records mapped to the given bean
	 * @see #select(SerializableFunction, Column, Consumer, Consumer)
	 * @see #newQuery(SQLBuilder, Class)
	 */
	public <C, I, J, T extends Table> List<C> select(SerializableBiFunction<I, J, C> factory, Column<T, I> column1, Column<T, J> column2,
													 Consumer<SelectMapping<C, T>> selectMapping) {
		return select(factory, column1, column2, selectMapping, where -> {});
	}
	
	/**
	 * Queries the database and invokes given no-arg constructor for each row.
	 * Additional bean fulfillment will be done by using configuration you'll give through {@link SelectMapping}.
	 *
	 * Usage is for very simple cases : only columns table is targeted (no join nor assembly are processed).
	 * Prefer {@link #newQuery(SQLBuilder, Class)} for a more complete use case.
	 *
	 * @param factory a two-arguments bean constructor
	 * @param <C> type of created beans
	 * @param <T> targeted table type
	 * @return a list of all table records mapped to the given bean
	 * @see #select(SerializableBiFunction, Column, Column, Consumer, Consumer)
	 * @see #newQuery(SQLBuilder, Class)
	 */
	public <C, T extends Table> List<C> select(SerializableSupplier<C> factory, Consumer<SelectMapping<C, T>> selectMapping,
							  Consumer<CriteriaChain> where) {
		Constructor<C> constructor = new MethodReferenceCapturer().findConstructor(factory);
		return select(constructor.getDeclaringClass(), queryMapper -> queryMapper.mapKey(factory), Collections.emptyList(), selectMapping, where);
	}
	
	/**
	 * Queries the database for given column and invokes given 1-arg constructor with it.
	 * Additional bean fulfillment will be done by using configuration you'll give through {@link SelectMapping}. 
	 *
	 * Usage is for very simple cases : only columns table is targeted (no join nor assembly are processed).
	 * Prefer {@link #newQuery(SQLBuilder, Class)} for a more complete use case.
	 *
	 * @param factory a one-argument bean constructor
	 * @param column any table column (primary key may be preferred because its result is given to bean constructor but it is not expected)
	 * @param selectMapping allow to add some mapping beyond instantiation time
	 * @param <C> type of created beans
	 * @param <I> constructor arg and column types
	 * @param <T> targeted table type
	 * @return a list of all table records mapped to the given bean
	 * @see #newQuery(SQLBuilder, Class)
	 */
	public <C, I, T extends Table> List<C> select(SerializableFunction<I, C> factory, Column<T, I> column,
												  Consumer<SelectMapping<C, T>> selectMapping,
												  Consumer<CriteriaChain> where) {
		Constructor<C> constructor = new MethodReferenceCapturer().findConstructor(factory);
		return select(constructor.getDeclaringClass(), queryMapper -> queryMapper.mapKey(factory, column), Collections.singletonList(column), selectMapping, where);
	}
	
	/**
	 * Queries the database for given columns and invokes given 2-args constructor with them.
	 * Additional bean fulfillment will be done by using configuration you'll give through {@link SelectMapping}.
	 *
	 * Usage is for very simple cases : only columns table is targeted (no join nor assembly are processed).
	 * Prefer {@link #newQuery(SQLBuilder, Class)} for a more complete use case.
	 *
	 * @param factory a one-argument bean constructor
	 * @param column1 a table column (may be a primary key column because its result is given to bean constructor but it is not expected)
	 * @param column2 a table column (may be a primary key column because its result is given to bean constructor but it is not expected)
	 * @param selectMapping allow to add some mapping beyond instantiation time
	 * @param <C> type of created beans
	 * @param <I> constructor first-arg type and first column type
	 * @param <J> constructor second-arg type and second column type
	 * @param <T> targeted table type
	 * @return a list of all table records mapped to the given bean
	 * @see #newQuery(SQLBuilder, Class)
	 */
	public <C, I, J, T extends Table> List<C> select(SerializableBiFunction<I, J, C> factory, Column<T, I> column1, Column<T, J> column2,
												  Consumer<SelectMapping<C, T>> selectMapping,
												  Consumer<CriteriaChain> where) {
		Constructor<C> constructor = new MethodReferenceCapturer().findConstructor(factory);
		return select(constructor.getDeclaringClass(), queryMapper -> queryMapper.mapKey(factory, column1, column2), Arrays.asList(column1, column2), selectMapping, where
		);
	}
	
	/**
	 * Very generic internal method made to execute selection
	 * 
	 * @param beanType bean type to be built, used for marking return type 
	 * @param keyMapper function that configures constructor and data key
	 * @param selectableKeys contains columns to be added to select clause
	 * @param selectMapping function that configures bean fulfillment
	 * @param where function that configures where clause
	 * @param <C> type of created beans
	 * @return beans created with selected data
	 */
	private <C, T extends Table> List<C> select(Class<C> beanType,
							   Consumer<BeanKeyQueryMapper<C>> keyMapper,
							   List<? extends Column<?, ?>> selectableKeys,
							   Consumer<SelectMapping<C, T>> selectMapping,
							   Consumer<CriteriaChain> where) {
		SelectMapping<C, T> selectMappingSupport = new SelectMapping<>();
		selectMapping.accept(selectMappingSupport);
		Table table = selectMappingSupport.getTable();
		if (table == null) {
			throw new IllegalArgumentException("Table is not defined, please add some columns to query so it can be deduced from them");
		}
		Query query = QueryEase.select(selectableKeys).from(table).getQuery();
		where.accept(query.getWhere());
		QueryMapper<C> queryMapper = newTransformableQuery(new QuerySQLBuilder(query), beanType);
		keyMapper.accept(queryMapper);
		selectMappingSupport.appendTo(query, queryMapper);
		return execute(queryMapper);
	}
	
	public <C> List<C> execute(QueryMapper<C> queryProvider) {
		return queryProvider.execute(getConnectionProvider());
	}
	
	public <C> C executeUnique(QueryMapper<C> queryProvider) {
		return queryProvider.executeUnique(getConnectionProvider());
	}
	
	public <T extends Table> ExecutableUpdate update(T table) {
		return new ExecutableUpdate(table);
	}
	
	public <T extends Table> ExecutableInsert<T> insert(T table) {
		return new ExecutableInsert<>(table);
	}
	
	public <T extends Table> ExecutableDelete delete(T table) {
		return new ExecutableDelete(table);
	}
	
	/**
	 * Small support to store additional bean fulfillment of select queries.
	 * 
	 * @param <C> resulting bean type
	 * @param <T> targeted table type of selected columns
	 */
	public static class SelectMapping<C, T extends Table> {
		
		private final Map<Column<Table, ?>, SerializableBiConsumer<C, ?>> mapping = new HashMap<>();
		
		/**
		 * Will add given {@link Column} to select clause and gives its data as input of given setter.
		 * Use it to fill properties of created bean through their setter.
		 * 
		 * @param column table column to be added to select clause
		 * @param setter property setter
		 * @param <O>
		 * @return
		 */
		public <O> SelectMapping<C, T> add(Column<T, O> column, SerializableBiConsumer<C, O> setter) {
			mapping.put((Column) column, setter);
			return this;
		}
		
		private void appendTo(Query query, BeanPropertyQueryMapper<C> queryMapper) {
			query.select(mapping.keySet());
			mapping.forEach((k, v) -> queryMapper.map((Column) k, v));
		}
		
		/**
		 * Gives column {@link Table}. Implementation takes from "first" column since they are expected to be all from same table.
		 * Only visible from {@link PersistenceContext} (not for external usage) because only necessary in case of absence of explicit selected columns
		 * (bean with no-arg constructor).
		 * May returns null if no {@link Column} is defined which should raise an exception since it doesn't make sense (remember : this method is
		 * only used when no-arg constructor is used, hence giving no additional bean fulfillment doesn't seem logical because that would create
		 * one (empty) bean per row : what's the use case ?)
		 * 
		 * @return the {@link Table} targeted by {@link Column}s defined in this instance, null if no {@link Column} are defined WHICH IS A WRONG USAGE
		 */
		@javax.annotation.Nullable
		private T getTable() {
			return (T) Nullable.nullable(Iterables.first(mapping.keySet())).map(Column::getTable).get();
		}
	}
	
	public class ExecutableUpdate extends Update {
		
		private ExecutableUpdate(Table targetTable) {
			super(targetTable);
		}
		
		/** Overriden to adapt return type */
		@Override
		public ExecutableUpdate set(Column column) {
			super.set(column);
			return this;
		}
		
		/** Overridden to adapt return type */
		@Override
		public <C> ExecutableUpdate set(Column<Table, C> column, C value) {
			super.set(column, value);
			return this;
		}
		
		/** Overridden to adapt return type */
		@Override
		public <C> ExecutableUpdate set(Column<Table, C> column1, Column<Table, C> column2) {
			super.set(column1, column2);
			return this;
		}
		
		/**
		 * Executes this update statement with given values
		 */
		public void execute() {
			UpdateStatement updateStatement = new UpdateCommandBuilder(this).toStatement(dialect.getColumnBinderRegistry());
			try (WriteOperation<Integer> writeOperation = dialect.getWriteOperationFactory().createInstance(updateStatement, getConnectionProvider())) {
				writeOperation.setValues(updateStatement.getValues());
				writeOperation.execute();
			}
		}
		
		@Override
		public ExecutableCriteria where(Column column, String condition) {
			CriteriaChain where = super.where(column, condition);
			return new MethodReferenceDispatcher()
					.redirect(ExecutableSQL::execute, this::execute)
					.redirect(CriteriaChain.class, where, true)
					.fallbackOn(this).build(ExecutableCriteria.class);
		}
		
		@Override
		public ExecutableCriteria where(Column column, AbstractRelationalOperator condition) {
			CriteriaChain where = super.where(column, condition);
			return new MethodReferenceDispatcher()
					.redirect(ExecutableSQL::execute, this::execute)
					.redirect(CriteriaChain.class, where, true)
					.fallbackOn(this).build(ExecutableCriteria.class);
		}
	}
	
	public class ExecutableInsert<T extends Table> extends Insert<T> {
		
		private ExecutableInsert(T table) {
			super(table);
		}
		
		/** Overridden to adapt return type */
		@Override
		public <C> ExecutableInsert<T> set(Column<T, C> column, C value) {
			super.set(column, value);
			return this;
		}
		
		/**
		 * Executes this insert statement.
		 */
		public void execute() {
			InsertStatement<T> insertStatement = new InsertCommandBuilder<>(this).toStatement(dialect.getColumnBinderRegistry());
			try (WriteOperation<Integer> writeOperation = dialect.getWriteOperationFactory().createInstance(insertStatement, getConnectionProvider())) {
				writeOperation.setValues(insertStatement.getValues());
				writeOperation.execute();
			}
		}
	}
	
	public class ExecutableDelete extends Delete {
		
		private ExecutableDelete(Table table) {
			super(table);
		}
		
		/**
		 * Executes this delete statement with given values.
		 */
		public void execute() {
			PreparedSQL deleteStatement = new DeleteCommandBuilder(this).toStatement(dialect.getColumnBinderRegistry());
			try (WriteOperation<Integer> writeOperation = dialect.getWriteOperationFactory().createInstance(deleteStatement, getConnectionProvider())) {
				writeOperation.setValues(deleteStatement.getValues());
				writeOperation.execute();
			}
		}
		
		@Override
		public ExecutableCriteria where(Column column, String condition) {
			CriteriaChain where = super.where(column, condition);
			return new MethodReferenceDispatcher()
					.redirect(ExecutableSQL::execute, this::execute)
					.redirect(CriteriaChain.class, where, true)
					.fallbackOn(this).build(ExecutableCriteria.class);
		}
		
		@Override
		public ExecutableCriteria where(Column column, AbstractRelationalOperator condition) {
			CriteriaChain where = super.where(column, condition);
			return new MethodReferenceDispatcher()
					.redirect(ExecutableSQL::execute, this::execute)
					.redirect(CriteriaChain.class, where, true)
					.fallbackOn(this).build(ExecutableCriteria.class);
		}
	}
	
	public interface ExecutableSQL {
		
		void execute();
	}
	
	public interface ExecutableCriteria extends CriteriaChain<ExecutableCriteria>, ExecutableSQL {
		
	}
	
	public interface ExecutableBeanPropertyKeyQueryMapper<C> extends BeanKeyQueryMapper<C>, ExecutableQuery<C> {
		
		<I> ExecutableBeanPropertyQueryMapper<C> mapKey(SerializableFunction<I, C> javaBeanCtor, String columnName);
		
		<I, J> ExecutableBeanPropertyQueryMapper<C> mapKey(SerializableBiFunction<I, J, C> javaBeanCtor, String columnName1, String columnName2);
		
		<I, J, K> ExecutableBeanPropertyQueryMapper<C> mapKey(SerializableTriFunction<I, J, K, C> javaBeanCtor,
															  String columnName1,
															  String columnName2,
															  String columnName3);
		
		ExecutableBeanPropertyQueryMapper<C> mapKey(SerializableSupplier<C> javaBeanCtor);
		
		<I> ExecutableBeanPropertyQueryMapper<C> mapKey(SerializableFunction<I, C> javaBeanCtor, String columnName, Class<I> columnType);
		
		<I, J> ExecutableBeanPropertyQueryMapper<C> mapKey(SerializableBiFunction<I, J, C> javaBeanCtor,
														   String column1Name, Class<I> column1Type,
														   String column2Name, Class<J> column2Type);
		
		<I, J, K> ExecutableBeanPropertyQueryMapper<C> mapKey(SerializableTriFunction<I, J, K, C> javaBeanCtor,
															  String column1Name, Class<I> column1Type,
															  String column2Name, Class<J> column2Type,
															  String column3Name, Class<K> column3Type);
		
		<I> ExecutableBeanPropertyQueryMapper<C> mapKey(SerializableFunction<I, C> javaBeanCtor, Column<? extends Table, I> column);
		
		<I, J> ExecutableBeanPropertyQueryMapper<C> mapKey(SerializableBiFunction<I, J, C> javaBeanCtor,
														   Column<? extends Table, I> column1,
														   Column<? extends Table, J> column2);
		
		<I, J, K> ExecutableBeanPropertyQueryMapper<C> mapKey(SerializableTriFunction<I, J, K, C> javaBeanCtor,
															  Column<? extends Table, I> column1,
															  Column<? extends Table, J> column2,
															  Column<? extends Table, K> column3
		);
		
		<I> ExecutableBeanPropertyQueryMapper<I> mapKey(String columnName, Class<I> columnType);
		
	}
	
	public interface ExecutableBeanPropertyQueryMapper<C> extends ExecutableQuery<C>, BeanPropertyQueryMapper<C> {
		
		@Override
		<I> ExecutableBeanPropertyQueryMapper<C> map(String columnName, SerializableBiConsumer<C, I> setter, Class<I> columnType);
		
		@Override
		<I, J> ExecutableBeanPropertyQueryMapper<C> map(String columnName, SerializableBiConsumer<C, J> setter, Class<I> columnType, Converter<I, J> converter);
		
		@Override
		<I> ExecutableBeanPropertyQueryMapper<C> map(String columnName, SerializableBiConsumer<C, I> setter);
		
		@Override
		<I, J> ExecutableBeanPropertyQueryMapper<C> map(String columnName, SerializableBiConsumer<C, J> setter, Converter<I, J> converter);
		
		@Override
		<I> ExecutableBeanPropertyQueryMapper<C> map(Column<? extends Table, I> column, SerializableBiConsumer<C, I> setter);
		
		@Override
		<I, J> ExecutableBeanPropertyQueryMapper<C> map(Column<? extends Table, I> column, SerializableBiConsumer<C, J> setter, Converter<I, J> converter);
		
		@Override
		<K, V> ExecutableBeanPropertyQueryMapper<C> map(BeanRelationFixer<C, V> combiner, ResultSetRowTransformer<K, V> relatedBeanCreator);
		
		@Override
		default ExecutableBeanPropertyQueryMapper<C> map(ResultSetRowAssembler<C> assembler) {
			return map(assembler, AssemblyPolicy.ON_EACH_ROW);
		}
		
		@Override
		ExecutableBeanPropertyQueryMapper<C> map(ResultSetRowAssembler<C> assembler, AssemblyPolicy assemblyPolicy);
		
		@Override
		ExecutableBeanPropertyQueryMapper<C> set(String paramName, Object value);
		
		@Override
		<O> ExecutableBeanPropertyQueryMapper<C> set(String paramName, O value, Class<? super O> valueType);
		
		@Override
		<O> ExecutableBeanPropertyQueryMapper<C> set(String paramName, Iterable<O> value, Class<? super O> valueType);
		
		/**
		 * Forces query to return very first result
		 * @return a configurable query that will only return first result when executed
		 */
		SingleResultExecutableSelect<C> singleResult();
	}
	
	/**
	 * Configurable query that will only return first result when executed
	 * @param <C> type of object returned by query execution
	 */
	public interface SingleResultExecutableSelect<C> extends BeanPropertyQueryMapper<C> {
		
		@Override
		<I> SingleResultExecutableSelect<C> map(String columnName, SerializableBiConsumer<C, I> setter, Class<I> columnType);
		
		@Override
		<I, J> SingleResultExecutableSelect<C> map(String columnName, SerializableBiConsumer<C, J> setter, Class<I> columnType, Converter<I, J> converter);
		
		@Override
		<I> SingleResultExecutableSelect<C> map(String columnName, SerializableBiConsumer<C, I> setter);
		
		@Override
		<I, J> SingleResultExecutableSelect<C> map(String columnName, SerializableBiConsumer<C, J> setter, Converter<I, J> converter);
		
		@Override
		<I> SingleResultExecutableSelect<C> map(Column<? extends Table, I> column, SerializableBiConsumer<C, I> setter);
		
		@Override
		<I, J> SingleResultExecutableSelect<C> map(Column<? extends Table, I> column, SerializableBiConsumer<C, J> setter, Converter<I, J> converter);
		
		@Override
		<K, V> SingleResultExecutableSelect<C> map(BeanRelationFixer<C, V> combiner, ResultSetRowTransformer<K, V> relatedBeanCreator);
		
		@Override
		default SingleResultExecutableSelect<C> map(ResultSetRowAssembler<C> assembler) {
			return (SingleResultExecutableSelect<C>) map(assembler, AssemblyPolicy.ON_EACH_ROW);
		}
		
		@Override
		SingleResultExecutableSelect<C> set(String paramName, Object value);
		
		@Override
		<O> SingleResultExecutableSelect<C> set(String paramName, O value, Class<? super O> valueType);
		
		@Override
		<O> SingleResultExecutableSelect<C> set(String paramName, Iterable<O> value, Class<? super O> valueType);
		
		C execute();
	}
	
	/**
	 * Bridge between {@link ConnectionProvider}, {@link ConnectionConfiguration}, {@link org.codefilarete.stalactite.sql.TransactionObserver}
	 * and {@link SeparateTransactionExecutor} so one can notify {@link PersistenceContext} from commit and rollback as well as maintain internal
	 * mechanisms such as :
	 * - creating a separate transaction to manage HiLo Sequence
	 * - revert entity version on transaction rollback (when versioning is active)
	 */
	private static class TransactionAwareConnectionConfiguration extends TransactionAwareConnectionProvider
			implements ConnectionConfiguration,
			SeparateTransactionExecutor	// for PooledHiLoSequence
	{
		
		private final ConnectionConfiguration connectionConfiguration;
		
		private final Nullable<SeparateTransactionExecutor> separateTransactionExecutor;
		
		private TransactionAwareConnectionConfiguration(ConnectionConfiguration connectionConfiguration) {
			super(connectionConfiguration.getConnectionProvider());
			this.connectionConfiguration = connectionConfiguration;
			// We'll be a real SeparateTransactionExecutor if given ConnectionProvider is one, else an exception will be thrown
			// at runtime when requiring-feature is invoked 
			if (connectionConfiguration.getConnectionProvider() instanceof SeparateTransactionExecutor) {
				this.separateTransactionExecutor = Nullable.nullable((SeparateTransactionExecutor) connectionConfiguration.getConnectionProvider());
			} else {
				this.separateTransactionExecutor = Nullable.empty();
			}
		}
		
		@Override
		public ConnectionProvider getConnectionProvider() {
			// because we gave delegate instance at construction time we return ourselves, hence returning instance we'll benefit from transaction
			// management through TransactionObserver
			return this;
		}
		
		@Override
		public int getBatchSize() {
			// we simply delegate information to ConnectionConfiguration instance
			return connectionConfiguration.getBatchSize();
		}
		
		@Override
		public void executeInNewTransaction(JdbcOperation jdbcOperation) {
			separateTransactionExecutor
					.invoke(executor -> executor.executeInNewTransaction(jdbcOperation))
					// in fact we're not really a SeparateTransactionExecutor because given ConnectionProvider is not one
					.elseThrow(new RuntimeException("Can't execute operation in separate transaction"
							+ " because connection provider doesn't implement " + Reflections.toString(SeparateTransactionExecutor.class)));
		}
	}
	
}
