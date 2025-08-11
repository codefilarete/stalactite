package org.codefilarete.stalactite.engine.runtime;

import java.sql.ResultSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.codefilarete.stalactite.engine.configurer.PersisterBuilderContext;
import org.codefilarete.stalactite.engine.configurer.PersisterBuilderImpl.BuildLifeCycleListener;
import org.codefilarete.stalactite.engine.runtime.load.EntityInflater;
import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree;
import org.codefilarete.stalactite.engine.runtime.load.EntityTreeInflater;
import org.codefilarete.stalactite.engine.runtime.load.EntityTreeQueryBuilder;
import org.codefilarete.stalactite.engine.runtime.load.EntityTreeQueryBuilder.EntityTreeQuery;
import org.codefilarete.stalactite.query.ConfiguredEntityCriteria;
import org.codefilarete.stalactite.query.EntityFinder;
import org.codefilarete.stalactite.query.builder.QuerySQLBuilderFactory.QuerySQLBuilder;
import org.codefilarete.stalactite.query.model.CriteriaChain;
import org.codefilarete.stalactite.query.model.GroupBy;
import org.codefilarete.stalactite.query.model.Having;
import org.codefilarete.stalactite.query.model.Limit;
import org.codefilarete.stalactite.query.model.LimitAware;
import org.codefilarete.stalactite.query.model.OrderBy;
import org.codefilarete.stalactite.query.model.OrderByChain;
import org.codefilarete.stalactite.query.model.Query;
import org.codefilarete.stalactite.query.model.Select;
import org.codefilarete.stalactite.query.model.Selectable;
import org.codefilarete.stalactite.query.model.Where;
import org.codefilarete.stalactite.sql.ConnectionProvider;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.Accumulator;
import org.codefilarete.stalactite.sql.result.ColumnedRow;
import org.codefilarete.stalactite.sql.result.ColumnedRowIterator;
import org.codefilarete.stalactite.sql.statement.PreparedSQL;
import org.codefilarete.stalactite.sql.statement.ReadOperation;
import org.codefilarete.stalactite.sql.statement.SQLExecutionException;
import org.codefilarete.stalactite.sql.statement.SQLOperation.SQLOperationListener;
import org.codefilarete.stalactite.sql.statement.SQLStatement;
import org.codefilarete.stalactite.sql.statement.StringParamedSQL;
import org.codefilarete.stalactite.sql.statement.binder.PreparedStatementWriter;
import org.codefilarete.stalactite.sql.statement.binder.ResultSetReader;
import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.tool.collection.KeepOrderMap;
import org.codefilarete.tool.collection.Maps;

import static org.codefilarete.stalactite.query.model.Operators.in;
import static org.codefilarete.tool.bean.Objects.preventNull;

/**
 * Class aimed at loading an entity graph which is selected by some criteria on some properties coming from a {@link CriteriaChain}.
 * 
 * Implementation is based on {@link EntityJoinTree} to build the query and the entity graph.
 * 
 * @author Guillaume Mary
 * @see EntityFinder#select(ConfiguredEntityCriteria, Consumer, Consumer, Map)
 */
public class RelationalEntityFinder<C, I, T extends Table<T>> implements EntityFinder<C, I> {
	
	private static final String PRIMARY_KEY_ALIAS = "rootId";
	
	private final EntityJoinTree<C, I> entityJoinTree;
	
	private final ConnectionProvider connectionProvider;
	
	private final Dialect dialect;
	
	private EntityTreeQuery<C> entityTreeQuery;
	
	private SQLOperationListener<?> operationListener;
	
	public RelationalEntityFinder(EntityJoinTree<C, I> entityJoinTree,
								  ConnectionProvider connectionProvider,
								  Dialect dialect) {
		this.entityJoinTree = entityJoinTree;
		this.connectionProvider = connectionProvider;
		this.dialect = dialect;
		
		PersisterBuilderContext.CURRENT.get().addBuildLifeCycleListener(new BuildLifeCycleListener() {
			@Override
			public void afterBuild() {
			}
			
			@Override
			public void afterAllBuild() {
				buildQuery();
			}
		});
	}
	
	public RelationalEntityFinder(EntityJoinTree<C, I> entityJoinTree,
								  ConnectionProvider connectionProvider,
								  Dialect dialect,
								  boolean withImmediateQueryBuild) {
		this.entityJoinTree = entityJoinTree;
		this.connectionProvider = connectionProvider;
		this.dialect = dialect;
		this.entityTreeQuery = new EntityTreeQueryBuilder<>(this.entityJoinTree, dialect.getColumnBinderRegistry()).buildSelectQuery();;
	}
	
	private void buildQuery() {
		entityTreeQuery = new EntityTreeQueryBuilder<>(this.entityJoinTree, dialect.getColumnBinderRegistry()).buildSelectQuery();
	}
	
	@Override
	public void setOperationListener(SQLOperationListener<?> operationListener) {
		this.operationListener = operationListener;
	}
	
	@Override
	public EntityJoinTree<C, I> getEntityJoinTree() {
		return entityJoinTree;
	}
	
	public Set<C> selectFromQueryBean(String sql, Map<String, Object> values) {
		// Computing parameter binders from values
		Map<String, PreparedStatementWriter<?>> parameterBinders = new HashMap<>();
		values.forEach((paramName, value) -> {
			PreparedStatementWriter<?> writer = dialect.getColumnBinderRegistry().getWriter(value.getClass());
			parameterBinders.put(paramName, writer);
		});
		
		return selectFromQueryBean(sql, values, parameterBinders);
	}
	
	public Set<C> selectFromQueryBean(String sql, Map<String, Object> values, Map<String, PreparedStatementWriter<?>> parameterBinders) {
		// we use EntityTreeQueryBuilder to get the inflater, please note that it also build the default Query
		EntityTreeInflater<C> inflater = entityTreeQuery.getInflater();
		
		// computing SQL readers from dialect binder registry
		Query query = entityTreeQuery.getQuery();
		Map<Selectable<?>, ResultSetReader<?>> selectParameterBinders = new HashMap<>();
		Map<Selectable<?>, String> aliases = new HashMap<>();
		query.getColumns().forEach(selectable -> {
			ResultSetReader<?> reader;
			String alias = preventNull(query.getAliases().get(selectable), selectable.getExpression());
			if (selectable instanceof Column) {
				reader = dialect.getColumnBinderRegistry().getReader((Column) selectable);
				selectParameterBinders.put(selectable, reader);
			} else {
				reader = dialect.getColumnBinderRegistry().getReader(selectable.getJavaType());
			}
			selectParameterBinders.put(selectable, reader);
			aliases.put(selectable, alias);
		});
		
		StringParamedSQL statement = new StringParamedSQL(sql, parameterBinders);
		statement.setValues(values);
		return new InternalExecutor(inflater, selectParameterBinders, aliases).execute(statement);
	}
	
	/**
	 * Implementation note: the load is done in 2 phases: one for root ids selection from criteria, a second for the whole graph load from found root ids.
	 */
	@Override
	public Set<C> select(ConfiguredEntityCriteria where,
						 Consumer<OrderByChain<?>> orderByClauseConsumer,
						 Consumer<LimitAware<?>> limitAwareConsumer,
						 Map<String, Object> valuesPerParam) {
		
		// we clone the query to avoid polluting the instance one, else, from select(..) to select(..), we append the criteria at the end of it,
		// which makes the query usually returning no data (because of the condition mix)
		Query queryClone = new Query(
				entityTreeQuery.getQuery().getSelectDelegate(),
				entityTreeQuery.getQuery().getFromDelegate(),
				new Where(),
				new GroupBy(),
				new Having(),
				new OrderBy(),
				new Limit());
		
		QuerySQLBuilder sqlQueryBuilder = dialect.getQuerySQLBuilderFactory().queryBuilder(queryClone, where.getCriteria());
		
		// When the condition contains some criteria on a collection, the ResultSet contains only data matching it,
		// then the graph is a partial view of the real entity. Therefore, when the condition contains some Collection criteria
		// we must load the graph in 2 phases: a first lookup for ids matching the result, and a second phase that loads the entity graph
		// according to the ids
		if (where.hasCollectionCriteria()) {
			// First phase: selecting ids (made by clearing selected elements for performance issue)
			KeepOrderMap<Selectable<?>, String> columns = queryClone.getSelectDelegate().clear();
			Column<T, I> pk = (Column<T, I>) Iterables.first(((Table) entityJoinTree.getRoot().getTable()).getPrimaryKey().getColumns());
			queryClone.select(pk, PRIMARY_KEY_ALIAS);
			Map<Column<?, ?>, String> aliases = Maps.asMap(pk, PRIMARY_KEY_ALIAS);
			Map<Column<?, ?>, ResultSetReader<?>> columnReaders = Maps.asMap(pk, dialect.getColumnBinderRegistry().getBinder(pk));
			orderByClauseConsumer.accept(queryClone.orderBy());
			limitAwareConsumer.accept(queryClone.orderBy());
			Set<I> ids = readIds(sqlQueryBuilder.toPreparableSQL().toPreparedSQL(new HashMap<>()), columnReaders, aliases);
			
			if (ids.isEmpty()) {
				// No result found, we must stop here because request below doesn't support in(..) without values (SQL error from database)
				return Collections.emptySet();
			} else {
				// Second phase : selecting elements by main table pk (adding necessary columns)
				queryClone.getSelectDelegate().remove(pk);    // previous pk selection removal
				columns.forEach(queryClone::select);
				queryClone.getWhereDelegate().clear();
				queryClone.where(pk, in(ids));
				
				PreparedSQL preparedSQL = sqlQueryBuilder.toPreparableSQL().toPreparedSQL(valuesPerParam);
				return new InternalExecutor(entityTreeQuery).execute(preparedSQL);
			}
		} else {
			// The condition doesn't have a criteria on a collection property (*-to-many): the load can be done with one query because the SQL criteria
			// doesn't make a subset of the entity graph
			orderByClauseConsumer.accept(queryClone.orderBy());
			limitAwareConsumer.accept(queryClone.orderBy());
			PreparedSQL preparedSQL = sqlQueryBuilder.toPreparableSQL().toPreparedSQL(valuesPerParam);
			return new InternalExecutor(entityTreeQuery).execute(preparedSQL);
		}
	}
	
	private Set<I> readIds(PreparedSQL preparedSQL, Map<Column<?, ?>, ResultSetReader<?>> columnReaders, Map<Column<?, ?>, String> aliases) {
		EntityInflater<C, I> entityInflater = entityJoinTree.getRoot().getEntityInflater();
		try (ReadOperation<Integer> closeableOperation = dialect.getReadOperationFactory().createInstance(preparedSQL, connectionProvider)) {
			ColumnedRowIterator rowIterator = new ColumnedRowIterator(closeableOperation.execute(), columnReaders, aliases);
			return Iterables.collect(() -> rowIterator, row -> entityInflater.giveIdentifier(row), HashSet::new);
		} catch (RuntimeException e) {
			throw new SQLExecutionException(preparedSQL.getSQL(), e);
		}
	}
	
	@Override
	public <R, O> R selectProjection(Consumer<Select> selectAdapter,
									 Accumulator<? super Function<Selectable<O>, O>, Object, R> accumulator,
									 CriteriaChain where,
									 boolean distinct,
									 Consumer<OrderByChain<?>> orderByClauseConsumer, Consumer<LimitAware<?>> limitAwareConsumer) {
		EntityTreeQuery<C> entityTreeQuery = new EntityTreeQueryBuilder<>(this.entityJoinTree, dialect.getColumnBinderRegistry()).buildSelectQuery();
		Query query = entityTreeQuery.getQuery();
		query.getSelectDelegate().setDistinct(distinct);
		orderByClauseConsumer.accept(query.getQuery().orderBy());
		
		
		QuerySQLBuilder sqlQueryBuilder = dialect.getQuerySQLBuilderFactory().queryBuilder(query, where);
		
		// First phase : selecting ids (made by clearing selected elements for performance issue)
		selectAdapter.accept(query.getSelectDelegate());
		Map<Selectable<?>, String> aliases = query.getAliases();
		
		Map<Selectable<?>, ResultSetReader<?>> columnReaders = Iterables.map(query.getColumns(), Function.identity(), selectable -> dialect.getColumnBinderRegistry().getBinder(selectable.getJavaType()));
		
		PreparedSQL preparedSQL = sqlQueryBuilder.toPreparableSQL().toPreparedSQL(new HashMap<>());
		return readProjection(preparedSQL, columnReaders, query.getAliases(), accumulator);
	}
	
	private <R, O> R readProjection(PreparedSQL preparedSQL, Map<Selectable<?>, ResultSetReader<?>> columnReaders, Map<Selectable<?>, String> aliases, Accumulator<? super Function<Selectable<O>, O>, Object, R> accumulator) {
		try (ReadOperation<Integer> closeableOperation = dialect.getReadOperationFactory().createInstance(preparedSQL, connectionProvider)) {
			ColumnedRowIterator rowIterator = new ColumnedRowIterator(closeableOperation.execute(), columnReaders, aliases);
			return accumulator.collect(Iterables.stream(rowIterator).map(row -> (Function<Selectable<O>, O>) row::get).collect(Collectors.toList()));
		} catch (RuntimeException e) {
			throw new SQLExecutionException(preparedSQL.getSQL(), e);
		}
	}
	
	/**
	 * Small class to avoid passing {@link EntityTreeQuery} as argument to all methods
	 */
	private class InternalExecutor {
		
		private final EntityTreeInflater<C> inflater;
		private final Map<Selectable<?>, ResultSetReader<?>> selectParameterBinders;
		private final Map<Selectable<?>, String> columnAliases;
		
		private InternalExecutor(EntityTreeQuery<C> entityTreeQuery) {
			this(entityTreeQuery.getInflater(), entityTreeQuery.getSelectParameterBinders(), entityTreeQuery.getColumnAliases());
		}
		
		public InternalExecutor(EntityTreeInflater<C> inflater,
								Map<Selectable<?>, ? extends ResultSetReader<?>> selectParameterBinders,
								Map<Selectable<?>, String> columnAliases) {
			this.inflater = inflater;
			this.selectParameterBinders = (Map<Selectable<?>, ResultSetReader<?>>) selectParameterBinders;
			this.columnAliases = columnAliases;
		}
		
		protected <ParamType> Set<C> execute(SQLStatement<ParamType> query) {
			try (ReadOperation<ParamType> readOperation = dialect.getReadOperationFactory().createInstance(query, connectionProvider)) {
				readOperation.setListener((SQLOperationListener<ParamType>) operationListener);
				// Note that setValues must be done after operationListener set
				readOperation.setValues(query.getValues());
				return transform(readOperation);
			} catch (RuntimeException e) {
				throw new SQLExecutionException(query.getSQL(), e);
			}
		}
		
		protected Set<C> transform(ReadOperation<?> closeableOperation) {
			ResultSet resultSet = closeableOperation.execute();
			// NB: we give the same ParametersBinders of those given at ColumnParameterizedSelect since the row iterator is expected to read column from it
			ColumnedRowIterator rowIterator = new ColumnedRowIterator(resultSet, selectParameterBinders, columnAliases);
			return transform(rowIterator);
		}
		
		protected Set<C> transform(Iterator<? extends ColumnedRow> rowIterator) {
			return inflater.transform(() -> (Iterator<ColumnedRow>) rowIterator, 50);
		}
	}
}
