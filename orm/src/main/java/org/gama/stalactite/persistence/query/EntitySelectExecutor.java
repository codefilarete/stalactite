package org.gama.stalactite.persistence.query;

import java.sql.ResultSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.gama.lang.collection.Iterables;
import org.gama.lang.collection.Maps;
import org.gama.stalactite.persistence.engine.cascade.JoinedStrategiesSelect;
import org.gama.stalactite.persistence.engine.cascade.StrategyJoinsRowTransformer;
import org.gama.stalactite.persistence.sql.dml.binder.ColumnBinderRegistry;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.query.builder.SQLQueryBuilder;
import org.gama.stalactite.query.model.CriteriaChain;
import org.gama.stalactite.query.model.Query;
import org.gama.stalactite.sql.ConnectionProvider;
import org.gama.stalactite.sql.dml.PreparedSQL;
import org.gama.stalactite.sql.dml.ReadOperation;
import org.gama.stalactite.sql.dml.SQLExecutionException;
import org.gama.stalactite.sql.result.Row;
import org.gama.stalactite.sql.result.RowIterator;

import static org.gama.stalactite.persistence.engine.cascade.JoinedStrategiesSelect.ROOT_STRATEGY_NAME;
import static org.gama.stalactite.query.model.Operators.in;

/**
 * Class for loading an entity graph which is selected by criteria on bean properties coming from {@link EntityCriteriaSupport}.
 * 
 * Implemented as a light version of {@link org.gama.stalactite.persistence.engine.cascade.JoinedStrategiesSelectExecutor} focused on {@link EntityCriteriaSupport},
 * hence it is based on {@link JoinedStrategiesSelect} to build the bean graph.
 * 
 * @author Guillaume Mary
 * @see #loadGraph(CriteriaChain)
 * @see #loadSelection(CriteriaChain)
 */
public class EntitySelectExecutor<C, I, T extends Table> implements IEntitySelectExecutor<C> {
	
	private static final String PRIMARY_KEY_ALIAS = "rootId";
	
	private final ConnectionProvider connectionProvider;
	
	/** Surrogate for joining strategies, will help to build the SQL */
	private final JoinedStrategiesSelect<C, I, T> joinedStrategiesSelect;
	
	private final ColumnBinderRegistry parameterBinderProvider;
	
	private final StrategyJoinsRowTransformer<C> rowTransformer;
	
	public EntitySelectExecutor(JoinedStrategiesSelect<C, I, T> joinedStrategiesSelect,
								ConnectionProvider connectionProvider,
								ColumnBinderRegistry columnBinderRegistry) {
		this(joinedStrategiesSelect, connectionProvider, columnBinderRegistry, new StrategyJoinsRowTransformer<>(joinedStrategiesSelect.getStrategyJoins(ROOT_STRATEGY_NAME)));
	}
	
	public EntitySelectExecutor(JoinedStrategiesSelect<C, I, T> joinedStrategiesSelect,
								ConnectionProvider connectionProvider,
								ColumnBinderRegistry columnBinderRegistry,
								StrategyJoinsRowTransformer<C> rowTransformer) {
		this.joinedStrategiesSelect = joinedStrategiesSelect;
		this.connectionProvider = connectionProvider;
		this.parameterBinderProvider = columnBinderRegistry;
		this.rowTransformer = rowTransformer;
		this.rowTransformer.setAliasProvider(this.joinedStrategiesSelect::getAlias);
	}
	
	/**
	 * Loads beans selected by the given criteria.
	 * <strong>Please note that as a difference from {@link #loadGraph(CriteriaChain)} only beans present in the selection will be loaded,
	 * which means that collections may be partial if criteria contain any criterion on their entity properties</strong>
	 * 
	 * @param where some criteria for graph selection
	 * @return beans loaded from rows selected by given criteria
	 */
	public List<C> loadSelection(CriteriaChain where) {
		SQLQueryBuilder sqlQueryBuilder = IEntitySelectExecutor.createQueryBuilder(where, joinedStrategiesSelect.buildSelectQuery());
		PreparedSQL preparedSQL = sqlQueryBuilder.toPreparedSQL(parameterBinderProvider);
		return execute(preparedSQL);
	}
	
	/**
	 * Loads a bean graph that matches given criteria.
	 * 
	 * <strong>Please note that as a difference from {@link #loadSelection(CriteriaChain)} all beans under aggregate root will be loaded
	 * (aggregate that matches criteria will be fully loaded)</strong>
	 * 
	 * Implementation note : the load is done in 2 phases : one for root ids selection from criteria, a second from full graph load from found root ids.
	 *
	 * @param where some criteria for aggregate selection
	 * @return root beans of aggregates that match criteria
	 */
	public List<C> loadGraph(CriteriaChain where) {
		Query query = joinedStrategiesSelect.buildSelectQuery();
		
		SQLQueryBuilder sqlQueryBuilder = IEntitySelectExecutor.createQueryBuilder(where, query);
		
		// First phase : selecting ids (made by clearing selected elements for performance issue)
		List<Object> columns = query.getSelectSurrogate().clear();
		Column<T, I> pk = (Column<T, I>) Iterables.first(joinedStrategiesSelect.getJoinsRoot().getTable().getPrimaryKey().getColumns());
		query.select(pk, PRIMARY_KEY_ALIAS);
		List<I> ids = readIds(sqlQueryBuilder, pk);
		
		if (ids.isEmpty()) {
			// No result found, we must stop here because request below doesn't support in(..) without values (SQL error from database)
			return Collections.emptyList();
		} else {
			// Second phase : selecting elements by main table pk (adding necessary columns)
			query.getSelectSurrogate().remove(0);    // previous pk selection removal
			columns.forEach(query::select);
			query.getWhereSurrogate().clear();
			query.where(pk, in(ids));
			
			PreparedSQL preparedSQL = sqlQueryBuilder.toPreparedSQL(parameterBinderProvider);
			return execute(preparedSQL);
		}
	}
	
	private List<I> readIds(SQLQueryBuilder sqlQueryBuilder, Column<T, I> pk) {
		PreparedSQL preparedSQL = sqlQueryBuilder.toPreparedSQL(parameterBinderProvider);
		try (ReadOperation<Integer> closeableOperation = new ReadOperation<>(preparedSQL, connectionProvider)) {
			ResultSet resultSet = closeableOperation.execute();
			RowIterator rowIterator = new RowIterator(resultSet, Maps.asMap(PRIMARY_KEY_ALIAS, parameterBinderProvider.getBinder(pk)));
			return Iterables.collectToList(() -> rowIterator, row -> (I) row.get(PRIMARY_KEY_ALIAS));
		} catch (RuntimeException e) {
			throw new SQLExecutionException(preparedSQL.getSQL(), e);
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
		return this.rowTransformer.transform(() -> rowIterator, 50, new HashMap<>());
	}
}
