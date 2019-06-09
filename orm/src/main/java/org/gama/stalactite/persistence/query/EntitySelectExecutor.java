package org.gama.stalactite.persistence.query;

import java.sql.ResultSet;
import java.util.Iterator;
import java.util.List;

import org.gama.sql.ConnectionProvider;
import org.gama.sql.dml.PreparedSQL;
import org.gama.sql.dml.ReadOperation;
import org.gama.sql.dml.SQLExecutionException;
import org.gama.sql.result.Row;
import org.gama.sql.result.RowIterator;
import org.gama.stalactite.persistence.engine.cascade.JoinedStrategiesSelect;
import org.gama.stalactite.persistence.engine.cascade.StrategyJoinsRowTransformer;
import org.gama.stalactite.persistence.sql.dml.binder.ColumnBinderRegistry;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.query.builder.QueryBuilder;
import org.gama.stalactite.query.model.Query;

import static org.gama.stalactite.persistence.engine.cascade.JoinedStrategiesSelect.FIRST_STRATEGY_NAME;

/**
 * Class for loading an entity graph which are selected by criteria on bean properties coming from {@link EntityCriteriaSupport}.
 * 
 * Implemented as a light version of {@link org.gama.stalactite.persistence.engine.cascade.JoinedStrategiesSelectExecutor} focused on {@link EntityCriteriaSupport},
 * hence it is based on {@link JoinedStrategiesSelect} to build the bean graph.
 * 
 * @author Guillaume Mary
 * @see #select(EntityCriteriaSupport)
 */
public class EntitySelectExecutor<C, I, T extends Table> {
	
	private final ConnectionProvider connectionProvider;
	/** The surrogate for joining the strategies, will help to build the SQL */
	private final JoinedStrategiesSelect<C, I, T> joinedStrategiesSelect;
	private final ColumnBinderRegistry parameterBinderProvider;
	
	public EntitySelectExecutor(JoinedStrategiesSelect<C, I, T> joinedStrategiesSelect, ConnectionProvider connectionProvider,
								ColumnBinderRegistry columnBinderRegistry) {
		this.parameterBinderProvider = columnBinderRegistry;
		this.connectionProvider = connectionProvider;
		this.joinedStrategiesSelect = joinedStrategiesSelect;
	}
	
	public List<C> select(EntityCriteriaSupport<C> entityCriteria) {
		Query query = joinedStrategiesSelect.buildSelectQuery();
		
		QueryBuilder queryBuilder = new QueryBuilder(query);
		query.getWhere().and(entityCriteria.getQuery());
		
		PreparedSQL preparedSQL = queryBuilder.toPreparedSQL(parameterBinderProvider);
		return execute(preparedSQL);
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
