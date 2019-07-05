package org.gama.stalactite.persistence.query;

import java.sql.ResultSet;
import java.util.Iterator;
import java.util.List;

import org.gama.lang.collection.Iterables;
import org.gama.lang.collection.Maps;
import org.gama.sql.ConnectionProvider;
import org.gama.sql.dml.PreparedSQL;
import org.gama.sql.dml.ReadOperation;
import org.gama.sql.dml.SQLExecutionException;
import org.gama.sql.result.Row;
import org.gama.sql.result.RowIterator;
import org.gama.stalactite.persistence.engine.cascade.JoinedStrategiesSelect;
import org.gama.stalactite.persistence.engine.cascade.StrategyJoinsRowTransformer;
import org.gama.stalactite.persistence.sql.dml.binder.ColumnBinderRegistry;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.query.builder.SQLQueryBuilder;
import org.gama.stalactite.query.model.CriteriaChain;
import org.gama.stalactite.query.model.Query;

import static org.gama.stalactite.persistence.engine.cascade.JoinedStrategiesSelect.FIRST_STRATEGY_NAME;
import static org.gama.stalactite.query.model.Operators.in;

/**
 * Class for loading an entity graph which is selected by criteria on bean properties coming from {@link EntityCriteriaSupport}.
 * 
 * Implemented as a light version of {@link org.gama.stalactite.persistence.engine.cascade.JoinedStrategiesSelectExecutor} focused on {@link EntityCriteriaSupport},
 * hence it is based on {@link JoinedStrategiesSelect} to build the bean graph.
 * 
 * @author Guillaume Mary
 * @see #loadSelection(EntityCriteriaSupport)
 */
public class EntitySelectExecutor<C, I, T extends Table> {
	
	private static final String PRIMARY_KEY_ALIAS = "rootId";
	
	private final ConnectionProvider connectionProvider;
	
	/** Surrogate for joining strategies, will help to build the SQL */
	private final JoinedStrategiesSelect<C, I, T> joinedStrategiesSelect;
	
	private final ColumnBinderRegistry parameterBinderProvider;
	
	public EntitySelectExecutor(JoinedStrategiesSelect<C, I, T> joinedStrategiesSelect,
								ConnectionProvider connectionProvider,
								ColumnBinderRegistry columnBinderRegistry) {
		this.joinedStrategiesSelect = joinedStrategiesSelect;
		this.connectionProvider = connectionProvider;
		this.parameterBinderProvider = columnBinderRegistry;
	}
	
	/**
	 * Loads beans selected by the given criteria.
	 * <strong>Please note that as a difference from {@link #loadGraph(EntityCriteriaSupport)} only beans present in the selection will be loaded,
	 * which means that collections may be partial if criteria contain any criterion on their entity properties</strong>
	 * 
	 * @param entityCriteria some criteria for graph selection
	 * @return beans loaded from rows selected by given criteria
	 */
	public List<C> loadSelection(EntityCriteriaSupport<C> entityCriteria) {
		SQLQueryBuilder SQLQueryBuilder = createQueryBuilder(entityCriteria, joinedStrategiesSelect.buildSelectQuery());
		PreparedSQL preparedSQL = SQLQueryBuilder.toPreparedSQL(parameterBinderProvider);
		return execute(preparedSQL);
	}
	
	/**
	 * Loads a bean graph that matches given criteria.
	 * 
	 * <strong>Please note that as a difference from {@link #loadSelection(EntityCriteriaSupport)} all beans under aggregate root will be loaded
	 * (aggregate that matches criteria will be fully loaded)</strong>
	 * 
	 * Implementation note : the load is done in 2 phases : one for root ids selection from criteria, a second from full graph load from root ids.
	 *
	 * @param entityCriteria some criteria for aggregate selection
	 * @return root beans of aggregates that match criteria
	 */
	public List<C> loadGraph(EntityCriteriaSupport<C> entityCriteria) {
		Query query = joinedStrategiesSelect.buildSelectQuery();
		
		SQLQueryBuilder sqlQueryBuilder = createQueryBuilder(entityCriteria, query);
		
		// First phase : selecting ids (made by clearing selected elements for performance issue)
		List<Object> columns = query.getSelectSurrogate().clear();
		Column<T, I> pk = (Column<T, I>) Iterables.first(joinedStrategiesSelect.getJoinsRoot().getTable().getPrimaryKey().getColumns());
		query.select(pk, PRIMARY_KEY_ALIAS);
		List<I> ids = readIds(sqlQueryBuilder, pk);
		
		// Second phase : selecting elements by main table pk (adding necessary columns)
		query.getSelectSurrogate().remove(0);	// previous pk selection removal
		columns.forEach(query::select);
		query.getWhereSurrogate().clear();
		query.where(pk, in(ids));
		
		PreparedSQL preparedSQL = sqlQueryBuilder.toPreparedSQL(parameterBinderProvider);
		return execute(preparedSQL);
	}
	
	private SQLQueryBuilder createQueryBuilder(EntityCriteriaSupport<C> entityCriteria, Query query) {
		SQLQueryBuilder sqlQueryBuilder = new SQLQueryBuilder(query);
		CriteriaChain where = entityCriteria.getCriteria();
		if (where.iterator().hasNext()) {    // prevents from empty where causing malformed SQL
			query.getWhere().and(where);
		}
		return sqlQueryBuilder;
	}
	
	private List<I> readIds(SQLQueryBuilder SQLQueryBuilder, Column<T, I> pk) {
		ReadOperation<Integer> operation = new ReadOperation<>(SQLQueryBuilder.toPreparedSQL(parameterBinderProvider), connectionProvider);
		try (ReadOperation<Integer> closeableOperation = operation) {
			ResultSet resultSet = closeableOperation.execute();
			RowIterator rowIterator = new RowIterator(resultSet, Maps.asMap(PRIMARY_KEY_ALIAS, parameterBinderProvider.getBinder(pk)));
			return Iterables.collectToList(() -> rowIterator, row -> (I) row.get(PRIMARY_KEY_ALIAS));
		} catch (RuntimeException e) {
			throw new SQLExecutionException(operation.getSqlStatement().getSQL(), e);
		}
	}
	
	public List<C> execute(PreparedSQL query) {
		try (ReadOperation<Integer> readOperation = new ReadOperation<>(query, connectionProvider)) {
			return execute(readOperation);
		}
	}
	
	private List<C> execute(ReadOperation<Integer> operation) {
		try (ReadOperation<Integer> closeableOperation = operation) {
			return transform(closeableOperation);
		} catch (RuntimeException e) {
			throw new SQLExecutionException(operation.getSqlStatement().getSQL(), e);
		}
	}
	
	protected List<C> transform(ReadOperation<Integer> closeableOperation) {
		ResultSet resultSet = closeableOperation.execute();
		// NB: we give the same ParametersBinders of those given at ColumnParameterizedSelect since the row iterator is expected to read column from it
		RowIterator rowIterator = new RowIterator(resultSet, joinedStrategiesSelect.getSelectParameterBinders());
		return transform(rowIterator);
	}
	
	protected List<C> transform(Iterator<Row> rowIterator) {
		StrategyJoinsRowTransformer<C> strategyJoinsRowTransformer = new StrategyJoinsRowTransformer<>(joinedStrategiesSelect.getStrategyJoins(FIRST_STRATEGY_NAME));
		strategyJoinsRowTransformer.setAliases(this.joinedStrategiesSelect.getAliases());
		return strategyJoinsRowTransformer.transform(() -> rowIterator, 50);
	}
}
