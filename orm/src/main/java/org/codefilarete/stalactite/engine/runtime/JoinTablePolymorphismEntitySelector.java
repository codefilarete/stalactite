package org.codefilarete.stalactite.engine.runtime;

import java.sql.ResultSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree;
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
import org.codefilarete.stalactite.sql.result.RowIterator;
import org.codefilarete.stalactite.sql.statement.PreparedSQL;
import org.codefilarete.stalactite.sql.statement.ReadOperation;
import org.codefilarete.stalactite.sql.statement.SQLExecutionException;
import org.codefilarete.stalactite.sql.statement.binder.ResultSetReader;
import org.codefilarete.tool.Duo;
import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.tool.collection.KeepOrderMap;

/**
 * @author Guillaume Mary
 */
public class JoinTablePolymorphismEntitySelector<C, I, T extends Table<T>> implements EntitySelector<C, I> {
	
	private final Map<Class<C>, ConfiguredRelationalPersister<C, I>> persisterPerSubclass;
	private final T mainTable;
	private final EntityJoinTree<C, I> entityJoinTree;
	private final ConnectionProvider connectionProvider;
	private final Dialect dialect;
	
	public JoinTablePolymorphismEntitySelector(Map<? extends Class<C>, ? extends ConfiguredRelationalPersister<C, I>> persisterPerSubclass,
											   T mainTable,
											   EntityJoinTree<C, I> entityJoinTree,
											   ConnectionProvider connectionProvider,
											   Dialect dialect) {
		this.persisterPerSubclass = (Map<Class<C>, ConfiguredRelationalPersister<C, I>>) persisterPerSubclass;
		this.mainTable = mainTable;
		this.entityJoinTree = entityJoinTree;
		this.connectionProvider = connectionProvider;
		this.dialect = dialect;
	}
	
	@Override
	public Set<C> select(ConfiguredEntityCriteria where, Consumer<OrderByChain<?>> orderByClauseConsumer, Consumer<LimitAware<?>> limitAwareConsumer) {
		EntityTreeQuery<C> entityTreeQuery = new EntityTreeQueryBuilder<>(entityJoinTree, dialect.getColumnBinderRegistry()).buildSelectQuery();
		Query query = entityTreeQuery.getQuery();
		
		persisterPerSubclass.values().forEach(subclassPersister -> {
			query.getFrom().leftOuterJoin(mainTable.getPrimaryKey(), subclassPersister.getMainTable().getPrimaryKey());
			((T) subclassPersister.getMainTable()).getPrimaryKey().getColumns().forEach(column -> {
				query.select(column, column.getAlias());
			});
		});
		
		QuerySQLBuilder sqlQueryBuilder = dialect.getQuerySQLBuilderFactory().queryBuilder(query, where.getCriteria(), entityTreeQuery.getColumnClones());
		
		// selecting ids and their entity type
		Map<String, ResultSetReader> columnReaders = new HashMap<>();
		Map<Selectable<?>, String> aliases = query.getAliases();
		aliases.forEach((selectable, s) -> columnReaders.put(s, dialect.getColumnBinderRegistry().getBinder((Column) selectable)));
		ColumnedRow columnedRow = new ColumnedRow(aliases::get);
		Map<Class, Set<I>> idsPerSubtype = readIds(sqlQueryBuilder.toPreparedSQL(), columnReaders, columnedRow);
		
		Set<C> result = new HashSet<>();
		idsPerSubtype.forEach((k, v) -> result.addAll(persisterPerSubclass.get(k).select(v)));
		return result;
	}
	
	private Map<Class, Set<I>> readIds(PreparedSQL preparedSQL, Map<String, ResultSetReader> columnReaders, ColumnedRow columnedRow) {
		// Below we keep order of given entities mainly to get steady unit tests. Meanwhile, this may have performance
		// impacts but very difficult to measure
		Map<Class, Set<I>> result = new KeepOrderMap<>();
		try (ReadOperation readOperation = new ReadOperation<>(preparedSQL, connectionProvider)) {
			ResultSet resultSet = readOperation.execute();
			
			RowIterator resultSetIterator = new RowIterator(resultSet, columnReaders);
			resultSetIterator.forEachRemaining(row -> {
				
				// looking for entity type on row : we read each subclass PK and check for nullity. The non-null one is the 
				// right one
				Set<Entry<Class<C>, ConfiguredRelationalPersister<C, I>>> entries = persisterPerSubclass.entrySet();
				Duo<Class, I> duo = null;
				I identifier;
				for (Entry<Class<C>, ConfiguredRelationalPersister<C, I>> entry : entries) {
					identifier = entry.getValue().getMapping().getIdMapping().getIdentifierAssembler().assemble(row, columnedRow);
					if (identifier != null) {
						duo = new Duo<>(entry.getKey(), identifier);
						break;
					}
				}
				result.computeIfAbsent(duo.getLeft(), k -> new HashSet<>()).add(duo.getRight());
			});
			return result;
		} catch (RuntimeException e) {
			throw new SQLExecutionException(preparedSQL.getSQL(), e);
		}
	}
	
	@Override
	public <R, O> R selectProjection(Consumer<Select> selectAdapter, Accumulator<? super Function<Selectable<O>, O>, Object, R> accumulator, CriteriaChain where,
									 boolean distinct, Consumer<OrderByChain<?>> orderByClauseConsumer, Consumer<LimitAware<?>> limitAwareConsumer) {
		EntityTreeQuery<C> entityTreeQuery = new EntityTreeQueryBuilder<>(this.entityJoinTree, dialect.getColumnBinderRegistry()).buildSelectQuery();
		Query query = entityTreeQuery.getQuery();
		query.getSelectSurrogate().setDistinct(distinct);
		
		QuerySQLBuilder sqlQueryBuilder = dialect.getQuerySQLBuilderFactory().queryBuilder(query, where, entityTreeQuery.getColumnClones());
		
		// First phase : selecting ids (made by clearing selected elements for performance issue)
		selectAdapter.accept(query.getSelectSurrogate());
		Map<Selectable<?>, String> aliases = query.getAliases();
		ColumnedRow columnedRow = new ColumnedRow(aliases::get);
		
		Map<String, ResultSetReader<?>> columnReaders = Iterables.map(query.getColumns(), new AliasAsserter<>(aliases::get), selectable -> dialect.getColumnBinderRegistry().getBinder(selectable.getJavaType()));
		
		PreparedSQL preparedSQL = sqlQueryBuilder.toPreparedSQL();
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
