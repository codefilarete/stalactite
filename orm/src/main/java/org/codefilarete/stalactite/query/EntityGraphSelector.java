package org.codefilarete.stalactite.query;

import java.sql.ResultSet;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.codefilarete.stalactite.engine.runtime.ConfiguredPersister;
import org.codefilarete.stalactite.engine.runtime.EntityMappingTreeSelectExecutor;
import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree;
import org.codefilarete.stalactite.engine.runtime.load.EntityTreeQueryBuilder;
import org.codefilarete.stalactite.engine.runtime.load.EntityTreeQueryBuilder.EntityTreeQuery;
import org.codefilarete.stalactite.mapping.ColumnedRow;
import org.codefilarete.stalactite.query.builder.QuerySQLBuilderFactory.QuerySQLBuilder;
import org.codefilarete.stalactite.query.model.CriteriaChain;
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
import org.codefilarete.stalactite.sql.statement.binder.ResultSetReader;
import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.tool.collection.KeepOrderMap;
import org.codefilarete.tool.collection.Maps;
import org.codefilarete.tool.collection.Maps.ChainingMap;

import static org.codefilarete.stalactite.query.model.Operators.in;

/**
 * Class aimed at loading an entity graph which is selected by properties criteria coming from {@link CriteriaChain}.
 * 
 * Implemented as a light version of {@link EntityMappingTreeSelectExecutor} focused on {@link EntityCriteriaSupport},
 * hence it is based on {@link EntityJoinTree} to build the bean graph.
 * 
 * @author Guillaume Mary
 * @see #select(CriteriaChain)
 */
public class EntityGraphSelector<C, I, T extends Table> implements EntitySelector<C, I> {
	
	private static final String PRIMARY_KEY_ALIAS = "rootId";
	
	private final ConnectionProvider connectionProvider;
	
	private final Dialect dialect;
	
	private final ConfiguredPersister<C, I> entityPersister;
	
	private final EntityJoinTree<C, I> entityJoinTree;
	
	public EntityGraphSelector(ConfiguredPersister<C, I> entityPersister,
							   EntityJoinTree<C, I> entityJoinTree,
							   ConnectionProvider connectionProvider,
							   Dialect dialect) {
		this.entityPersister = entityPersister;
		this.entityJoinTree = entityJoinTree;
		this.connectionProvider = connectionProvider;
		this.dialect = dialect;
	}
	
	/**
	 * Implementation note : the load is done in 2 phases : one for root ids selection from criteria, a second from full graph load from found root ids.
	 */
	@Override
	public Set<C> select(CriteriaChain where) {
		EntityTreeQuery<C> entityTreeQuery = new EntityTreeQueryBuilder<>(this.entityJoinTree, dialect.getColumnBinderRegistry()).buildSelectQuery();
		Query query = entityTreeQuery.getQuery();
		
		QuerySQLBuilder sqlQueryBuilder = dialect.getQuerySQLBuilderFactory().queryBuilder(query, where, entityTreeQuery.getColumnClones());
		
		// First phase : selecting ids (made by clearing selected elements for performance issue)
		KeepOrderMap<Selectable<?>, String> columns = query.getSelectSurrogate().clear();
		Column<T, I> pk = (Column<T, I>) Iterables.first(((Table) entityJoinTree.getRoot().getTable()).getPrimaryKey().getColumns());
		query.select(pk, PRIMARY_KEY_ALIAS);
		ChainingMap<String, ResultSetReader> columnReaders = Maps.asMap(PRIMARY_KEY_ALIAS, dialect.getColumnBinderRegistry().getBinder(pk));
		Map<Column<?, ?>, String> aliases = Maps.asMap(pk, PRIMARY_KEY_ALIAS);
		ColumnedRow columnedRow = new ColumnedRow(aliases::get);
		Set<I> ids = readIds(sqlQueryBuilder.toPreparedSQL(), columnReaders, columnedRow);
		
		if (ids.isEmpty()) {
			// No result found, we must stop here because request below doesn't support in(..) without values (SQL error from database)
			return Collections.emptySet();
		} else {
			// Second phase : selecting elements by main table pk (adding necessary columns)
			query.getSelectSurrogate().remove(pk);    // previous pk selection removal
			columns.forEach(query::select);
			query.getWhereSurrogate().clear();
			query.where(pk, in(ids));
			
			PreparedSQL preparedSQL = sqlQueryBuilder.toPreparedSQL();
			return new InternalExecutor(entityTreeQuery).execute(preparedSQL);
		}
	}
	
	private Set<I> readIds(PreparedSQL preparedSQL, Map<String, ResultSetReader> columnReaders, ColumnedRow columnedRow) {
		try (ReadOperation<Integer> closeableOperation = new ReadOperation<>(preparedSQL, connectionProvider)) {
			ResultSet resultSet = closeableOperation.execute();
			RowIterator rowIterator = new RowIterator(resultSet, columnReaders);
			return Iterables.collect(() -> rowIterator, row -> entityPersister.getMapping().getIdMapping().getIdentifierAssembler().assemble(row, columnedRow), HashSet::new);
		} catch (RuntimeException e) {
			throw new SQLExecutionException(preparedSQL.getSQL(), e);
		}
	}
	
	@Override
	public <R, O> R selectProjection(Consumer<Select> selectAdapter, Accumulator<? super Function<Selectable<O>, O>, Object, R> accumulator, CriteriaChain where, boolean distinct) {
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
	 * Small class to avoid passing {@link EntityTreeQuery} as argument to all methods
	 */
	private class InternalExecutor {
		
		private final EntityTreeQuery<C> entityTreeQuery;
		
		private InternalExecutor(EntityTreeQuery<C> entityTreeQuery) {
			this.entityTreeQuery = entityTreeQuery;
		}
		
		protected Set<C> execute(PreparedSQL query) {
			try (ReadOperation<Integer> readOperation = new ReadOperation<>(query, connectionProvider)) {
				return execute(readOperation);
			}
		}
		
		private Set<C> execute(ReadOperation<Integer> operation) {
			try (ReadOperation<Integer> closeableOperation = operation) {
				return transform(closeableOperation);
			} catch (RuntimeException e) {
				throw new SQLExecutionException(operation.getSqlStatement().getSQL(), e);
			}
		}
		
		protected Set<C> transform(ReadOperation<Integer> closeableOperation) {
			ResultSet resultSet = closeableOperation.execute();
			// NB: we give the same ParametersBinders of those given at ColumnParameterizedSelect since the row iterator is expected to read column from it
			RowIterator rowIterator = new RowIterator(resultSet, entityTreeQuery.getSelectParameterBinders());
			return transform(rowIterator);
		}
		
		protected Set<C> transform(Iterator<Row> rowIterator) {
			return this.entityTreeQuery.getInflater().transform(() -> rowIterator, 50);
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
