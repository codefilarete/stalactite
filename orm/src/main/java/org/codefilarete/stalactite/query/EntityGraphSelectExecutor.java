package org.codefilarete.stalactite.query;

import java.sql.ResultSet;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.codefilarete.stalactite.engine.runtime.EntityMappingTreeSelectExecutor;
import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree;
import org.codefilarete.stalactite.engine.runtime.load.EntityTreeQueryBuilder;
import org.codefilarete.stalactite.engine.runtime.load.EntityTreeQueryBuilder.EntityTreeQuery;
import org.codefilarete.stalactite.query.builder.QuerySQLBuilderFactory.QuerySQLBuilder;
import org.codefilarete.stalactite.query.model.CriteriaChain;
import org.codefilarete.stalactite.query.model.Query;
import org.codefilarete.stalactite.query.model.Selectable;
import org.codefilarete.stalactite.sql.ConnectionProvider;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.Row;
import org.codefilarete.stalactite.sql.result.RowIterator;
import org.codefilarete.stalactite.sql.statement.PreparedSQL;
import org.codefilarete.stalactite.sql.statement.ReadOperation;
import org.codefilarete.stalactite.sql.statement.SQLExecutionException;
import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.tool.collection.KeepOrderMap;
import org.codefilarete.tool.collection.Maps;

import static org.codefilarete.stalactite.query.model.Operators.in;

/**
 * Class aimed at loading an entity graph which is selected by properties criteria coming from {@link EntityCriteriaSupport}.
 * 
 * Implemented as a light version of {@link EntityMappingTreeSelectExecutor} focused on {@link EntityCriteriaSupport},
 * hence it is based on {@link EntityJoinTree} to build the bean graph.
 * 
 * @author Guillaume Mary
 * @see #loadGraph(CriteriaChain)
 */
public class EntityGraphSelectExecutor<C, I, T extends Table> implements EntitySelectExecutor<C> {
	
	private static final String PRIMARY_KEY_ALIAS = "rootId";
	
	private final ConnectionProvider connectionProvider;
	
	private final Dialect dialect;
	
	private final EntityJoinTree<C, I> entityJoinTree;
	
	public EntityGraphSelectExecutor(EntityJoinTree<C, I> entityJoinTree,
									 ConnectionProvider connectionProvider,
									 Dialect dialect) {
		this.entityJoinTree = entityJoinTree;
		this.connectionProvider = connectionProvider;
		this.dialect = dialect;
	}
	
	/**
	 * Loads a bean graph that matches given criteria.
	 * 
	 * <strong>Please note that all beans under aggregate root will be loaded (aggregate that matches criteria will be fully loaded)</strong>
	 * 
	 * Implementation note : the load is done in 2 phases : one for root ids selection from criteria, a second from full graph load from found root ids.
	 *
	 * @param where some criteria for aggregate selection
	 * @return root beans of aggregates that match criteria
	 */
	@Override
	public Set<C> loadGraph(CriteriaChain where) {
		EntityTreeQuery<C> entityTreeQuery = new EntityTreeQueryBuilder<>(this.entityJoinTree, dialect.getColumnBinderRegistry()).buildSelectQuery();
		Query query = entityTreeQuery.getQuery();
		
		QuerySQLBuilder sqlQueryBuilder = dialect.getQuerySQLBuilderFactory().queryBuilder(query, where, entityTreeQuery.getColumnClones());
		
		// First phase : selecting ids (made by clearing selected elements for performance issue)
		KeepOrderMap<Selectable<?>, String> columns = query.getSelectSurrogate().clear();
		Column<T, I> pk = (Column<T, I>) Iterables.first(entityJoinTree.getRoot().getTable().getPrimaryKey().getColumns());
		query.select(pk, PRIMARY_KEY_ALIAS);
		List<I> ids = readIds(sqlQueryBuilder, pk);
		
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
	
	private List<I> readIds(QuerySQLBuilder sqlQueryBuilder, Column<T, I> pk) {
		PreparedSQL preparedSQL = sqlQueryBuilder.toPreparedSQL();
		try (ReadOperation<Integer> closeableOperation = new ReadOperation<>(preparedSQL, connectionProvider)) {
			ResultSet resultSet = closeableOperation.execute();
			RowIterator rowIterator = new RowIterator(resultSet, Maps.asMap(PRIMARY_KEY_ALIAS, dialect.getColumnBinderRegistry().getBinder(pk)));
			return Iterables.collectToList(() -> rowIterator, row -> (I) row.get(PRIMARY_KEY_ALIAS));
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
}
