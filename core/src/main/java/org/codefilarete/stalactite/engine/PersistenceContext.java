package org.codefilarete.stalactite.engine;

import javax.sql.DataSource;
import java.lang.reflect.Constructor;
import java.lang.reflect.Executable;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.LongStream;

import org.codefilarete.reflection.MethodReferenceCapturer;
import org.codefilarete.reflection.MethodReferenceDispatcher;
import org.codefilarete.stalactite.engine.PersisterRegistry.DefaultPersisterRegistry;
import org.codefilarete.stalactite.engine.runtime.BeanPersister;
import org.codefilarete.stalactite.query.builder.SQLBuilder;
import org.codefilarete.stalactite.query.model.ConditionalOperator;
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
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.order.Delete;
import org.codefilarete.stalactite.sql.order.DeleteCommandBuilder;
import org.codefilarete.stalactite.sql.order.Insert;
import org.codefilarete.stalactite.sql.order.InsertCommandBuilder;
import org.codefilarete.stalactite.sql.order.InsertCommandBuilder.InsertStatement;
import org.codefilarete.stalactite.sql.order.Update;
import org.codefilarete.stalactite.sql.order.Update.UpdateColumn;
import org.codefilarete.stalactite.sql.order.UpdateCommandBuilder;
import org.codefilarete.stalactite.sql.order.UpdateCommandBuilder.UpdateStatement;
import org.codefilarete.stalactite.sql.result.Accumulator;
import org.codefilarete.stalactite.sql.result.Accumulators;
import org.codefilarete.stalactite.sql.result.BeanRelationFixer;
import org.codefilarete.stalactite.sql.result.ResultSetRowAssembler;
import org.codefilarete.stalactite.sql.result.ResultSetRowTransformer;
import org.codefilarete.stalactite.sql.result.WholeResultSetTransformer.AssemblyPolicy;
import org.codefilarete.stalactite.sql.statement.PreparedSQL;
import org.codefilarete.stalactite.sql.statement.WriteOperation;
import org.codefilarete.tool.Nullable;
import org.codefilarete.tool.collection.Arrays;
import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.tool.collection.KeepOrderSet;
import org.codefilarete.tool.function.Converter;
import org.codefilarete.tool.function.SerializableTriFunction;
import org.danekja.java.util.function.serializable.SerializableBiConsumer;
import org.danekja.java.util.function.serializable.SerializableBiFunction;
import org.danekja.java.util.function.serializable.SerializableFunction;
import org.danekja.java.util.function.serializable.SerializableSupplier;

import static org.codefilarete.tool.function.Predicates.not;

/**
 * Entry point for persistence in a database. Mix of configuration (Transaction, Dialect, ...) and registry for {@link BeanPersister}s.
 *
 * @author Guillaume Mary
 * @see #PersistenceContext(DataSource)
 */
public class PersistenceContext implements DatabaseCrudOperations {
	
	private static final int DEFAULT_BATCH_SIZE = 100;
	
	private final ConnectionConfiguration configuration;
	private final Dialect dialect;
	private final DefaultPersisterRegistry persisterRegistry = new DefaultPersisterRegistry();
	
	/**
	 * Constructor with minimal but necessary information.
	 * JDBC batch size is set to {@value DEFAULT_BATCH_SIZE}.
	 * Dialect is deduced from JVM Service Provider and connection metadata.
	 * Connection is provided per Thread and <strong>autocommit is set to false hence transaction are not managed</strong> : caller must use
	 * {@link Connection#commit()} and {@link Connection#rollback()} to handle it. 
	 * 
	 * @param dataSource a JDBC {@link Connection} provider
	 * @see CurrentThreadConnectionProvider
	 */
	public PersistenceContext(DataSource dataSource) {
		this(new CurrentThreadTransactionalConnectionProvider(dataSource));
	}
	
	/**
	 * Constructor with minimal but necessary information.
	 * JDBC batch size is set to 100.
	 * Connection is provided per Thread and <strong>autocommit is set to false hence transaction are not managed</strong> : caller must use
	 * {@link Connection#commit()} and {@link Connection#rollback()} to handle it. 
	 *
	 * @param dataSource a JDBC {@link Connection} provider
	 * @param dialect dialect to be used with {@link Connection} given by dataSource
	 */
	public PersistenceContext(DataSource dataSource, Dialect dialect) {
		this(new CurrentThreadTransactionalConnectionProvider(dataSource), dialect);
	}
	
	/**
	 * Constructor with minimal but necessary information.
	 * JDBC batch size is set to {@value DEFAULT_BATCH_SIZE}.
	 * Dialect is deduced from JVM Service Provider and connection metadata.
	 *
	 * @param connectionProvider a JDBC {@link Connection} provider
	 */
	public PersistenceContext(ConnectionProvider connectionProvider) {
		this(new ConnectionConfigurationSupport(connectionProvider, DEFAULT_BATCH_SIZE), new ServiceLoaderDialectResolver());
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
	 * JDBC batch size is set to {@value DEFAULT_BATCH_SIZE}.
	 * 
	 * @param connectionProvider a JDBC {@link Connection} provider
	 * @param dialectResolver dialect provider
	 */
	public PersistenceContext(ConnectionProvider connectionProvider, DialectResolver dialectResolver) {
		this(new ConnectionConfigurationSupport(connectionProvider, DEFAULT_BATCH_SIZE), dialectResolver);
	}
	
	/**
	 * Constructor with {@link Connection} information and dialect provider.
	 *
	 * @param connectionConfiguration necessary information on JDBC {@link Connection}
	 * @param dialectResolver dialect provider
	 */
	public PersistenceContext(ConnectionConfiguration connectionConfiguration, DialectResolver dialectResolver) {
		this(connectionConfiguration, dialectResolver.determineDialect(connectionConfiguration.getConnectionProvider().giveConnection()));
	}
	
	/**
	 * Constructor with {@link Connection} provider and dialect.
	 * JDBC batch size is set to 100.
	 *
	 * @param connectionProvider a JDBC {@link Connection} provider
	 * @param dialect dialect to be used with {@link Connection}
	 */
	public PersistenceContext(ConnectionProvider connectionProvider, Dialect dialect) {
		this(new ConnectionConfigurationSupport(connectionProvider, DEFAULT_BATCH_SIZE), dialect);
	}
	
	/**
	 * Constructor with {@link Connection} information and dialect.
	 *
	 * @param connectionConfiguration necessary information on JDBC {@link Connection}
	 * @param dialect dialect to be used with {@link Connection}
	 */
	public PersistenceContext(ConnectionConfiguration connectionConfiguration, Dialect dialect) {
		this.configuration = connectionConfiguration;
		this.dialect =dialect;
	}
	
	public ConnectionProvider getConnectionProvider() {
		return configuration.getConnectionProvider();
	}
	
	public int getJDBCBatchSize() {
		return getConnectionConfiguration().getBatchSize();
	}
	
	public Dialect getDialect() {
		return dialect;
	}
	
	/**
	 * Gives the persister of given class if exists.
	 * Expected to work for aggregate root entity type, not for intermediary persisters, to keep a clean and sane behavior of cascades,
	 * bi-directionality, polymorphism, etc.
	 * Meanwhile, it depends on the context in which {@link #addPersister(EntityPersister)} was used.
	 * 
	 * @param clazz persister entity class
	 * @return null if no persister was found for given class
	 * @param <C> entity type
	 * @param <I> entity identifier type
	 */
	public <C, I> EntityPersister<C, I> getPersister(Class<C> clazz) {
		return persisterRegistry.getPersister(clazz);
	}
	
	/**
	 * Register a persister into this context. Then, it can be retrieved with {@link #getPersister(Class)}.
	 * It's expected to give it an aggregate root persister, not intermediary persisters, to keep a clean and sane behavior of cascades,
	 * bi-directionality, polymorphism, etc.
	 * 
	 * @param persister the persister to be registered
	 */
	public void addPersister(EntityPersister<?, ?> persister) {
		persisterRegistry.addPersister(persister);
	}
	
	public Set<EntityPersister> getPersisters() {
		return persisterRegistry.getPersisters();
	}
	
	public ConnectionConfiguration getConnectionConfiguration() {
		return this.configuration;
	}
	
	@Override
	public <C> ExecutableBeanPropertyKeyQueryMapper<C> newQuery(QueryProvider<Query> queryProvider, Class<C> beanType) {
		return newQuery(queryProvider.getQuery(), beanType);
	}
	
	@Override
	public <C> ExecutableBeanPropertyKeyQueryMapper<C> newQuery(Query query, Class<C> beanType) {
		return newQuery(this.dialect.getQuerySQLBuilderFactory().queryBuilder(query), beanType);
	}
	
	@Override
	public <C> ExecutableBeanPropertyKeyQueryMapper<C> newQuery(CharSequence sql, Class<C> beanType) {
		return newQuery(() -> sql, beanType);
	}
	
	@Override
	public <C> ExecutableBeanPropertyKeyQueryMapper<C> newQuery(SQLBuilder sql, Class<C> beanType) {
		return wrapIntoExecutable(newTransformableQuery(sql, beanType));
	}
	
	private <C> ExecutableBeanPropertyKeyQueryMapper<C> wrapIntoExecutable(QueryMapper<C> queryMapperSupport) {
		ExecutableBeanPropertyQueryMapper<C> beanPropertyMappingHandler = new MethodReferenceDispatcher()
				// since ExecutableBeanPropertyQueryMapper can be run we redirect execute() method to the underlying method
				.redirect((SerializableBiFunction<ExecutableQuery<C>, Accumulator<C, Object, Object>, Object>) ExecutableQuery::execute,
						(Function<Accumulator<C, Object, Object>, Object>) (accumulator) -> execute(queryMapperSupport, accumulator))
				.redirect(BeanPropertyQueryMapper.class, queryMapperSupport, true)
				.build((Class<ExecutableBeanPropertyQueryMapper<C>>) (Class) ExecutableBeanPropertyQueryMapper.class);
		
		return new MethodReferenceDispatcher()
				// BeanKeyQueryMapper methods are applied to a convenient instance but their results are redirected to another one to comply with their signature 
				.redirect(BeanKeyQueryMapper.class, queryMapperSupport, beanPropertyMappingHandler)
				// since ExecutableBeanPropertyKeyQueryMapper can be run we redirect execute() method to the underlying method
				.redirect((SerializableBiFunction<ExecutableQuery<C>, Accumulator<C, Object, Object>, Object>) ExecutableQuery::execute,
						(Function<Accumulator<C, Object, Object>, Object>) (accumulator) -> execute(queryMapperSupport, accumulator))
				.build((Class<ExecutableBeanPropertyKeyQueryMapper<C>>) (Class) ExecutableBeanPropertyKeyQueryMapper.class);
	}
	
	private <C> QueryMapper<C> newTransformableQuery(SQLBuilder sql, Class<C> beanType) {
		return new QueryMapper<>(beanType, sql, getDialect().getColumnBinderRegistry());
	}
	
	@Override
	public <C, I, T extends Table> Set<C> select(SerializableFunction<I, C> factory, Column<T, I> column) {
		Executable constructor = new MethodReferenceCapturer().findExecutable(factory);
		return newQuery(QueryEase
				.select(column).from(column.getTable()), ((Class<C>) constructor.getDeclaringClass()))
				.mapKey(factory, column)
				.execute(Accumulators.toSet());
	}
	
	@Override
	public <C, I, J, T extends Table> Set<C> select(SerializableBiFunction<I, J, C> factory, Column<T, I> column1, Column<T, J> column2) {
		Constructor<C> constructor = new MethodReferenceCapturer().findConstructor(factory);
		return newQuery(QueryEase.select(column1, column2).from(column1.getTable()), constructor.getDeclaringClass())
				.mapKey(factory, column1, column2)
				.execute(Accumulators.toSet());
	}
	
	@Override
	public <C, I, J, K, T extends Table> Set<C> select(SerializableTriFunction<I, J, K, C> factory, Column<T, I> column1, Column<T, J> column2, Column<T, K> column3) {
		Constructor<C> constructor = new MethodReferenceCapturer().findConstructor(factory);
		return newQuery(QueryEase.select(column1, column2, column3).from(column1.getTable()), constructor.getDeclaringClass())
				.mapKey(factory, column1, column2, column3)
				.execute(Accumulators.toSet());
	}
	
	@Override
	public <C, T extends Table> Set<C> select(SerializableSupplier<C> factory, Consumer<SelectMapping<C, T>> selectMapping) {
		return select(factory, selectMapping, where -> {});
	}
	
	@Override
	public <C, I, T extends Table> Set<C> select(SerializableFunction<I, C> factory, Column<T, I> column, Consumer<SelectMapping<C, T>> selectMapping) {
		return select(factory, column, selectMapping, where -> {});
	}
	
	@Override
	public <C, I, J, T extends Table> Set<C> select(SerializableBiFunction<I, J, C> factory, Column<T, I> column1, Column<T, J> column2,
													Consumer<SelectMapping<C, T>> selectMapping) {
		return select(factory, column1, column2, selectMapping, where -> {});
	}
	
	@Override
	public <C, T extends Table> Set<C> select(SerializableSupplier<C> factory, Consumer<SelectMapping<C, T>> selectMapping,
											  Consumer<CriteriaChain> where) {
		Constructor<C> constructor = new MethodReferenceCapturer().findConstructor(factory);
		return select(constructor.getDeclaringClass(), queryMapper -> queryMapper.mapKey(factory), Collections.emptySet(), selectMapping, where);
	}
	
	@Override
	public <C, I, T extends Table> Set<C> select(SerializableFunction<I, C> factory, Column<T, I> column,
												 Consumer<SelectMapping<C, T>> selectMapping,
												 Consumer<CriteriaChain> where) {
		Constructor<C> constructor = new MethodReferenceCapturer().findConstructor(factory);
		return select(constructor.getDeclaringClass(), queryMapper -> queryMapper.mapKey(factory, column), Arrays.asHashSet(column), selectMapping, where);
	}
	
	@Override
	public <C, I, J, T extends Table> Set<C> select(SerializableBiFunction<I, J, C> factory, Column<T, I> column1, Column<T, J> column2,
													Consumer<SelectMapping<C, T>> selectMapping,
													Consumer<CriteriaChain> where) {
		Constructor<C> constructor = new MethodReferenceCapturer().findConstructor(factory);
		return select(constructor.getDeclaringClass(), queryMapper -> queryMapper.mapKey(factory, column1, column2), Arrays.asHashSet(column1, column2), selectMapping, where);
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
	private <C, T extends Table> Set<C> select(Class<C> beanType,
											   Consumer<BeanKeyQueryMapper<C>> keyMapper,
											   Set<? extends Column<?, ?>> selectableKeys,
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
		QueryMapper<C> queryMapper = newTransformableQuery(dialect.getQuerySQLBuilderFactory().queryBuilder(query), beanType);
		keyMapper.accept(queryMapper);
		selectMappingSupport.appendTo(query, queryMapper);
		return execute(queryMapper);
	}
	
	private <C> Set<C> execute(QueryMapper<C> queryMapper) {
		return execute(queryMapper, Accumulators.toSet());
	}
	
	private <C, R> R execute(QueryMapper<C> queryMapper, Accumulator<C, ?, R> accumulator) {
		return queryMapper.execute(getConnectionProvider(), accumulator);
	}
	
	@Override
	public <T extends Table<T>> ExecutableUpdate<T> update(T table) {
		return new DefaultExecutableUpdate<>(table);
	}
	
	@Override
	public <T extends Table<T>> ExecutableInsert<T> insert(T table) {
		return new DefaultExecutableInsert<>(table);
	}
	
	@Override
	public <T extends Table> BatchInsert<T> batchInsert(T table) {
		return new DefaultBatchInsert<>(table);
	}
	
	@Override
	public <T extends Table<T>> ExecutableDelete<T> delete(T table) {
		return new DefaultExecutableDelete<>(table);
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
	
	private class DefaultExecutableUpdate<T extends Table<T>> extends Update<T> implements ExecutableUpdate<T> {
		
		private DefaultExecutableUpdate(T targetTable) {
			super(targetTable);
		}
		
		/** Overridden to adapt return type */
		@Override
		public <C> DefaultExecutableUpdate<T> set(Column<T, C> column, C value) {
			super.set(column, value);
			return this;
		}
		
		/** Overridden to adapt return type */
		@Override
		public <C> DefaultExecutableUpdate<T> set(Column<T, C> column1, Column<?, C> column2) {
			super.set(column1, column2);
			return this;
		}
		
		/**
		 * Executes this update statement with given values
		 */
		@Override
		public void execute() {
			UpdateStatement<T> updateStatement = new UpdateCommandBuilder<>(this, dialect).toStatement();
			try (WriteOperation<Integer> writeOperation = dialect.getWriteOperationFactory().createInstance(updateStatement, getConnectionProvider())) {
				writeOperation.setValues(updateStatement.getValues());
				writeOperation.execute();
			}
		}
		
		@Override
		public ExecutableCriteria where(Column<T, ?> column, String condition) {
			CriteriaChain where = super.where(column, condition);
			return new MethodReferenceDispatcher()
					.redirect(ExecutableSQL::execute, this::execute)
					.redirect(CriteriaChain.class, where, true)
					.fallbackOn(this).build(ExecutableCriteria.class);
		}
		
		@Override
		public ExecutableCriteria where(Column<T, ?> column, ConditionalOperator condition) {
			CriteriaChain where = super.where(column, condition);
			return new MethodReferenceDispatcher()
					.redirect(ExecutableSQL::execute, this::execute)
					.redirect(CriteriaChain.class, where, true)
					.fallbackOn(this).build(ExecutableCriteria.class);
		}
	}
	
	private class DefaultExecutableInsert<T extends Table<T>> extends Insert<T> implements ExecutableInsert<T> {
		
		private DefaultExecutableInsert(T table) {
			super(table);
		}
		
		@Override
		public <C> DefaultExecutableInsert<T> set(Column<T, C> column, C value) {
			super.set(column, value);
			return this;
		}
		
		@Override
		public void execute() {
			InsertStatement<T> insertStatement = new InsertCommandBuilder<>(this, dialect).toStatement();
			try (WriteOperation<Column<T, ?>> writeOperation = dialect.getWriteOperationFactory().createInstance(insertStatement, getConnectionProvider())) {
				writeOperation.setValues(insertStatement.getValues());
				writeOperation.execute();
			}
		}
	}
	
	private class DefaultBatchInsert<T extends Table<T>> extends Insert<T> implements BatchInsert<T> {
		
		private final List<Set<UpdateColumn<T>>> rows = new ArrayList<>();
		
		private DefaultBatchInsert(T table) {
			super(table);
		}
		
		@Override
		public <C> DefaultBatchInsert<T> set(Column<T, C> column, C value) {
			super.set(column, value);
			return this;
		}
		
		@Override
		public BatchInsert<T> newRow() {
			rows.add(new KeepOrderSet<>(getRow()));
			getRow().clear();
			return this;
		}
		
		@Override
		public long execute() {
			// treating remaining values in case user didn't call newRow(..)
			if (!getRow().isEmpty()) {
				rows.add(new KeepOrderSet<>(getRow()));
			}
			InsertStatement<T> insertStatement = new InsertCommandBuilder<>(this, dialect).toStatement();
			long[] writeCount;
			try (WriteOperation<Column<T, ?>> writeOperation = dialect.getWriteOperationFactory().createInstance(insertStatement, getConnectionProvider())) {
				this.rows.stream()
						// avoiding empty rows made by several calls to newRow(..) without setting values. Can happen if insert(..) is reused in a loop.
						.filter(not(Set::isEmpty))
						.<Map<Column<T, ?>, ?>>map(row -> Iterables.map(row, UpdateColumn::getColumn, UpdateColumn::getValue))
						.forEach(writeOperation::addBatch);
				writeCount = writeOperation.executeBatch();
			}
			// we clear current rows to let one reuse this instance
			rows.clear();
			getRow().clear();
			return LongStream.of(writeCount).sum();
		}
	}
	
	public class DefaultExecutableDelete<T extends Table<T>> extends Delete<T> implements ExecutableDelete<T> {
		
		private DefaultExecutableDelete(T table) {
			super(table);
		}
		
		@Override
		public void execute() {
			PreparedSQL deleteStatement = new DeleteCommandBuilder<>(this, dialect).toPreparableSQL().toPreparedSQL(new HashMap<>());
			try (WriteOperation<Integer> writeOperation = dialect.getWriteOperationFactory().createInstance(deleteStatement, getConnectionProvider())) {
				writeOperation.setValues(deleteStatement.getValues());
				writeOperation.execute();
			}
		}
		
		@Override
		public ExecutableCriteria where(Column<T, ?> column, String condition) {
			CriteriaChain where = super.where(column, condition);
			return new MethodReferenceDispatcher()
					.redirect(ExecutableSQL::execute, this::execute)
					.redirect(CriteriaChain.class, where, true)
					.fallbackOn(this).build(ExecutableCriteria.class);
		}
		
		@Override
		public ExecutableCriteria where(Column<T, ?> column, ConditionalOperator condition) {
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
		<I> ExecutableBeanPropertyQueryMapper<C> map(String columnName, BiConsumer<C, I> setter, Class<I> columnType);
		
		@Override
		<I, J> ExecutableBeanPropertyQueryMapper<C> map(String columnName, SerializableBiConsumer<C, J> setter, Class<I> columnType, Converter<I, J> converter);
		
		@Override
		<I> ExecutableBeanPropertyQueryMapper<C> map(String columnName, SerializableBiConsumer<C, I> setter);
		
		@Override
		<I, J> ExecutableBeanPropertyQueryMapper<C> map(String columnName, SerializableBiConsumer<C, J> setter, Converter<I, J> converter);
		
		@Override
		<I> ExecutableBeanPropertyQueryMapper<C> map(Column<? extends Table, I> column, BiConsumer<C, I> setter);
		
		@Override
		<I, J> ExecutableBeanPropertyQueryMapper<C> map(Column<? extends Table, I> column, BiConsumer<C, J> setter, Converter<I, J> converter);
		
		@Override
		<K, V> ExecutableBeanPropertyQueryMapper<C> map(BeanRelationFixer<C, V> combiner, ResultSetRowTransformer<V, K> relatedBeanCreator);
		
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
	}
}
