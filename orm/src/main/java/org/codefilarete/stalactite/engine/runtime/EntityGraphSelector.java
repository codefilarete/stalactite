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

import org.codefilarete.stalactite.engine.runtime.load.EntityInflater;
import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree;
import org.codefilarete.stalactite.engine.runtime.load.EntityTreeInflater;
import org.codefilarete.stalactite.engine.runtime.load.EntityTreeQueryBuilder;
import org.codefilarete.stalactite.engine.runtime.load.EntityTreeQueryBuilder.EntityTreeQuery;
import org.codefilarete.stalactite.mapping.ColumnedRow;
import org.codefilarete.stalactite.query.ConfiguredEntityCriteria;
import org.codefilarete.stalactite.query.EntitySelector;
import org.codefilarete.stalactite.query.builder.QuerySQLBuilderFactory.QuerySQLBuilder;
import org.codefilarete.stalactite.query.model.CriteriaChain;
import org.codefilarete.stalactite.query.model.LimitAware;
import org.codefilarete.stalactite.query.model.OrderByChain;
import org.codefilarete.stalactite.query.model.Query;
import org.codefilarete.stalactite.query.model.Select;
import org.codefilarete.stalactite.query.model.Selectable;
import org.codefilarete.stalactite.sql.ConnectionProvider;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.Accumulator;
import org.codefilarete.stalactite.sql.result.Row;
import org.codefilarete.stalactite.sql.result.RowIterator;
import org.codefilarete.stalactite.sql.statement.PreparedSQL;
import org.codefilarete.stalactite.sql.statement.ReadOperation;
import org.codefilarete.stalactite.sql.statement.SQLExecutionException;
import org.codefilarete.stalactite.sql.statement.SQLStatement;
import org.codefilarete.stalactite.sql.statement.StringParamedSQL;
import org.codefilarete.stalactite.sql.statement.binder.PreparedStatementWriter;
import org.codefilarete.stalactite.sql.statement.binder.ResultSetReader;
import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.tool.collection.KeepOrderMap;
import org.codefilarete.tool.collection.Maps;
import org.codefilarete.tool.collection.Maps.ChainingMap;

import static org.codefilarete.stalactite.query.model.Operators.in;
import static org.codefilarete.tool.bean.Objects.preventNull;

/**
 * Class aimed at loading an entity graph which is selected by properties criteria coming from {@link CriteriaChain}.
 * 
 * Implemented as a light version of {@link EntityMappingTreeSelectExecutor} focused on {@link EntityCriteriaSupport},
 * hence it is based on {@link EntityJoinTree} to build the bean graph.
 * 
 * @author Guillaume Mary
 * @see EntitySelector#select(ConfiguredEntityCriteria, Consumer, Consumer, Map)
 */
public class EntityGraphSelector<C, I, T extends Table<T>> implements EntitySelector<C, I> {
	
	private static final String PRIMARY_KEY_ALIAS = "rootId";
	
	private final EntityJoinTree<C, I> entityJoinTree;
	
	private final ConnectionProvider connectionProvider;
	
	private final Dialect dialect;
	
//	private final EntityTreeQuery<C> entityTreeQuery;
//	private final Query defaultQuery;
//	private final EntityTreeInflater<C> inflater;
	
	public EntityGraphSelector(EntityJoinTree<C, I> entityJoinTree,
							   ConnectionProvider connectionProvider,
							   Dialect dialect) {
		this.entityJoinTree = entityJoinTree;
		this.connectionProvider = connectionProvider;
		this.dialect = dialect;
//		// we use EntityTreeQueryBuilder to get the inflater, please note that it also build the default Query
//		this.entityTreeQuery = new EntityTreeQueryBuilder<>(this.entityJoinTree, dialect.getColumnBinderRegistry()).buildSelectQuery();
//		this.inflater = entityTreeQuery.getInflater();
//		this.defaultQuery = entityTreeQuery.getQuery();
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
		EntityTreeQuery<C> entityTreeQuery = new EntityTreeQueryBuilder<>(this.entityJoinTree, dialect.getColumnBinderRegistry())
				.buildSelectQuery();
		EntityTreeInflater<C> inflater = entityTreeQuery.getInflater();
		
		// computing SQL readers from dialect binder registry
		Query query = entityTreeQuery.getQuery();
		Map<String, ResultSetReader<?>> selectParameterBinders = new HashMap<>();
		query.getColumns().forEach(selectable -> {
			ResultSetReader<?> reader;
			String alias = preventNull(query.getAliases().get(selectable), selectable.getExpression());
			if (selectable instanceof Column) {
				reader = dialect.getColumnBinderRegistry().getReader((Column) selectable);
				selectParameterBinders.put(alias, reader);
			} else {
				reader = dialect.getColumnBinderRegistry().getReader(selectable.getJavaType());
			}
			selectParameterBinders.put(alias, reader);
		});
		
		StringParamedSQL statement = new StringParamedSQL(sql, parameterBinders);
		statement.setValues(values);
		return new InternalExecutor(inflater, selectParameterBinders).execute(statement);
	}
	
	/**
	 * Implementation note : the load is done in 2 phases : one for root ids selection from criteria, a second from full graph load from found root ids.
	 */
	@Override
	public Set<C> select(ConfiguredEntityCriteria where,
						 Consumer<OrderByChain<?>> orderByClauseConsumer,
						 Consumer<LimitAware<?>> limitAwareConsumer,
						 Map<String, Object> valuesPerParam) {
		EntityTreeQuery<C> entityTreeQuery = new EntityTreeQueryBuilder<>(this.entityJoinTree, dialect.getColumnBinderRegistry()).buildSelectQuery();
		Query query = entityTreeQuery.getQuery();
		
		QuerySQLBuilder sqlQueryBuilder = dialect.getQuerySQLBuilderFactory().queryBuilder(query, where.getCriteria(), entityTreeQuery.getColumnClones());
		ColumnCloneAwareOrderBy cloneAwareOrderBy = new ColumnCloneAwareOrderBy(query.orderBy(), entityTreeQuery.getColumnClones());
		
		// When condition contains some criteria on a collection, the ResultSet contains only data matching it,
		// then the graph is a partial view of the real entity. Therefore, when the condition contains some Collection criteria
		// we must load the graph in 2 phases : a first lookup for ids matching the result, and a second phase that loads the entity graph
		// according to the ids
		if (where.hasCollectionCriteria()) {
			// First phase : selecting ids (made by clearing selected elements for performance issue)
			KeepOrderMap<Selectable<?>, String> columns = query.getSelectSurrogate().clear();
			Column<T, I> pk = (Column<T, I>) Iterables.first(((Table) entityJoinTree.getRoot().getTable()).getPrimaryKey().getColumns());
			query.select(pk, PRIMARY_KEY_ALIAS);
			ChainingMap<String, ResultSetReader> columnReaders = Maps.asMap(PRIMARY_KEY_ALIAS, dialect.getColumnBinderRegistry().getBinder(pk));
			Map<Column<?, ?>, String> aliases = Maps.asMap(pk, PRIMARY_KEY_ALIAS);
			ColumnedRow columnedRow = new ColumnedRow(aliases::get);
			orderByClauseConsumer.accept(cloneAwareOrderBy);
			limitAwareConsumer.accept(query.orderBy());
			Set<I> ids = readIds(sqlQueryBuilder.toPreparableSQL().toPreparedSQL(new HashMap<>()), columnReaders, columnedRow);
			
			if (ids.isEmpty()) {
				// No result found, we must stop here because request below doesn't support in(..) without values (SQL error from database)
				return Collections.emptySet();
			} else {
				// Second phase : selecting elements by main table pk (adding necessary columns)
				query.getSelectSurrogate().remove(pk);    // previous pk selection removal
				columns.forEach(query::select);
				query.getWhereSurrogate().clear();
				query.where(pk, in(ids));
				
				PreparedSQL preparedSQL = sqlQueryBuilder.toPreparableSQL().toPreparedSQL(valuesPerParam);
				return new InternalExecutor(entityTreeQuery).execute(preparedSQL);
			}
		} else {
			// Condition doesn't have criteria on a collection property (*-to-many) : the load can be done with one query because the SQL criteria
			// doesn't make a subset of the entity graph
			orderByClauseConsumer.accept(cloneAwareOrderBy);
			limitAwareConsumer.accept(query.orderBy());
			PreparedSQL preparedSQL = sqlQueryBuilder.toPreparableSQL().toPreparedSQL(valuesPerParam);
			return new InternalExecutor(entityTreeQuery).execute(preparedSQL);
		}
	}
	
	private Set<I> readIds(PreparedSQL preparedSQL, Map<String, ResultSetReader> columnReaders, ColumnedRow columnedRow) {
		EntityInflater<C, I> entityInflater = entityJoinTree.getRoot().getEntityInflater();
		try (ReadOperation<Integer> closeableOperation = new ReadOperation<>(preparedSQL, connectionProvider)) {
			ResultSet resultSet = closeableOperation.execute();
			RowIterator rowIterator = new RowIterator(resultSet, columnReaders);
			return Iterables.collect(() -> rowIterator, row -> entityInflater.giveIdentifier(row, columnedRow), HashSet::new);
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
		query.getSelectSurrogate().setDistinct(distinct);
		orderByClauseConsumer.accept(query.getQuery().orderBy());
		
		
		QuerySQLBuilder sqlQueryBuilder = dialect.getQuerySQLBuilderFactory().queryBuilder(query, where, entityTreeQuery.getColumnClones());
		
		// First phase : selecting ids (made by clearing selected elements for performance issue)
		selectAdapter.accept(query.getSelectSurrogate());
		Map<Selectable<?>, String> aliases = query.getAliases();
		ColumnedRow columnedRow = new ColumnedRow(aliases::get);
		
		Map<String, ResultSetReader<?>> columnReaders = Iterables.map(query.getColumns(), new AliasAsserter<>(aliases::get), selectable -> dialect.getColumnBinderRegistry().getBinder(selectable.getJavaType()));
		
		PreparedSQL preparedSQL = sqlQueryBuilder.toPreparableSQL().toPreparedSQL(new HashMap<>());
		return readProjection(preparedSQL, columnReaders, columnedRow, accumulator);
	}
	
	private <R, O> R readProjection(PreparedSQL preparedSQL, Map<String, ResultSetReader<?>> columnReaders, ColumnedRow columnedRow, Accumulator<? super Function<Selectable<O>, O>, Object, R> accumulator) {
		try (ReadOperation<Integer> closeableOperation = new ReadOperation<>(preparedSQL, connectionProvider)) {
			ResultSet resultSet = closeableOperation.execute();
			RowIterator rowIterator = new RowIterator(resultSet, columnReaders);
			return accumulator.collect(Iterables.stream(rowIterator).map(row -> (Function<Selectable<O>, O>) selectable -> columnedRow.getValue(selectable, row)).collect(Collectors.toList()));
		} catch (RuntimeException e) {
			throw new SQLExecutionException(preparedSQL.getSQL(), e);
		}
	}
	
	/**
	 * Small class to avoid passing {@link EntityTreeQuery} as argument to all methods
	 */
	private class InternalExecutor {
		
		private final EntityTreeInflater<C> inflater;
		private final Map<String, ResultSetReader<?>> selectParameterBinders;
		
		private InternalExecutor(EntityTreeQuery<C> entityTreeQuery) {
			this(entityTreeQuery.getInflater(), entityTreeQuery.getSelectParameterBinders());
		}
		
		private InternalExecutor(EntityTreeInflater<C> inflater, Map<String, ? extends ResultSetReader<?>> selectParameterBinders) {
			this.inflater = inflater;
			this.selectParameterBinders = (Map<String, ResultSetReader<?>>) selectParameterBinders;
		}
		
		protected Set<C> execute(SQLStatement<?> query) {
			try (ReadOperation<?> readOperation = new ReadOperation<>(query, connectionProvider)) {
				return transform(readOperation);
			} catch (RuntimeException e) {
				throw new SQLExecutionException(query.getSQL(), e);
			}
		}
		
		protected Set<C> transform(ReadOperation<?> closeableOperation) {
			ResultSet resultSet = closeableOperation.execute();
			// NB: we give the same ParametersBinders of those given at ColumnParameterizedSelect since the row iterator is expected to read column from it
			RowIterator rowIterator = new RowIterator(resultSet, selectParameterBinders);
			return transform(rowIterator);
		}
		
		protected Set<C> transform(Iterator<Row> rowIterator) {
			return inflater.transform(() -> rowIterator, 50);
		}
	}
	
	/**
	 * Small class that will be used to ensure that a {@link Selectable} as an alias in the query
	 * @param <S>
	 * @author Guillaume Mary
	 */
	private static class AliasAsserter<S extends Selectable> implements Function<S, String> {
		
		private final Function<S, String> delegate;
		
		private AliasAsserter(Function<S, String> delegate) {
			this.delegate = delegate;
		}
		
		@Override
		public String apply(S selectable) {
			String alias = delegate.apply(selectable);
			if (alias == null) {
				throw new IllegalArgumentException("Item " + selectable.getExpression() + " must have an alias");
			}
			return alias;
		}
	}
}
