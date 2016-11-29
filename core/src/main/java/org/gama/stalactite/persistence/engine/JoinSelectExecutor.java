package org.gama.stalactite.persistence.engine;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.gama.lang.StringAppender;
import org.gama.lang.Strings;
import org.gama.lang.collection.Collections;
import org.gama.lang.collection.Iterables;
import org.gama.lang.collection.KeepOrderSet;
import org.gama.lang.exception.Exceptions;
import org.gama.sql.IConnectionProvider;
import org.gama.sql.SimpleConnectionProvider;
import org.gama.sql.binder.ParameterBinder;
import org.gama.sql.dml.ReadOperation;
import org.gama.sql.result.Row;
import org.gama.sql.result.RowIterator;
import org.gama.stalactite.persistence.engine.JoinedStrategySelect.StrategyJoin;
import org.gama.stalactite.persistence.mapping.ClassMappingStrategy;
import org.gama.stalactite.persistence.sql.Dialect;
import org.gama.stalactite.persistence.sql.dml.ColumnParamedSelect;
import org.gama.stalactite.persistence.structure.Table.Column;
import org.gama.stalactite.query.builder.SelectQueryBuilder;
import org.gama.stalactite.query.model.SelectQuery;

import static org.gama.sql.dml.ExpandableSQL.ExpandableParameter.SQL_PARAMETER_MARK_1;
import static org.gama.sql.dml.ExpandableSQL.ExpandableParameter.SQL_PARAMETER_MARK_10;
import static org.gama.sql.dml.ExpandableSQL.ExpandableParameter.SQL_PARAMETER_MARK_100;

/**
 * Class aimed at executing a SQL select statement from multiple joined {@link ClassMappingStrategy}
 * 
 * @author Guillaume Mary
 */
public class JoinSelectExecutor<T, I> {
	
	/** The surrogate for joining the strategies, will help to build the SQL */
	private final JoinedStrategySelect<T, I> joinedStrategySelect;
	private final Dialect dialect;
	private final Map<String, ParameterBinder> selectParameterBinders = new HashMap<>();
	private final Map<Column, ParameterBinder> parameterBinders = new HashMap<>();
	private final Map<Column, int[]> inOperatorValueIndexes = new HashMap<>();
	private final int blockSize;
	private final ConnectionProvider connectionProvider;
	
	private final Column keyColumn;
	private List<T> result;
	
	JoinSelectExecutor(ClassMappingStrategy<T, I> classMappingStrategy, Dialect dialect, ConnectionProvider connectionProvider) {
		this.joinedStrategySelect = new JoinedStrategySelect<>(classMappingStrategy, c -> dialect.getColumnBinderRegistry().getBinder(c));
		this.dialect = dialect;
		this.connectionProvider = connectionProvider;
		// post-initialization
		this.blockSize = dialect.getInOperatorMaxSize();
		this.keyColumn = classMappingStrategy.getTargetTable().getPrimaryKey();
		
	}
	
	public ConnectionProvider getConnectionProvider() {
		return connectionProvider;
	}

	public <U> void addComplementaryTables(ClassMappingStrategy<U, ?> mappingStrategy, Function<T, Iterable<U>> setter, Column leftJoinColumn, Column rightJoinColumn) {
		joinedStrategySelect.add(JoinedStrategySelect.FIRST_STRATEGY_NAME, mappingStrategy, leftJoinColumn, rightJoinColumn, setter);
	}
	
	private void addColumnsToSelect(KeepOrderSet<Column> selectableColumns, SelectQuery selectQuery, Map<String, ParameterBinder> selectParameterBinders) {
		for (Column selectableColumn : selectableColumns) {
			selectQuery.select(selectableColumn, selectableColumn.getAlias());
			selectParameterBinders.put(selectableColumn.getName(), dialect.getColumnBinderRegistry().getBinder(selectableColumn));
		}
	}
	
	public List<T> select(Iterable<I> ids) {
		// cutting ids into pieces, adjusting expected result size
		List<List<I>> parcels = Collections.parcel(ids, blockSize);
		result = new ArrayList<>(parcels.size() * blockSize);
		
		SelectQuery selectQuery = joinedStrategySelect.buildSelectQuery();
		SelectQueryBuilder queryBuilder = new SelectQueryBuilder(selectQuery);
		queryBuilder.toSQL();
		
		// Use same Connection for all operations
		IConnectionProvider connectionProvider = new SimpleConnectionProvider(getConnectionProvider().getCurrentConnection());
		// Creation of the where clause: we use a dynamic "in" operator clause to avoid multiple SelectQueryBuilder instanciation
		DynamicInClause condition = new DynamicInClause();
		selectQuery.where(keyColumn, condition);
		List<I> lastBlock = Iterables.last(parcels);
		// keep only full blocks to run them on the fully filled "in" operator
		int lastBlockSize = lastBlock.size();
		if (lastBlockSize != blockSize) {
			parcels = Collections.cutTail(parcels);
		}
		if (!parcels.isEmpty()) {
			// change parameter mark count to adapt "in" operator values
			condition.setParamMarkCount(blockSize);
			// adding "in" identifiers to where clause
			bindInClause(blockSize);
			execute(connectionProvider, queryBuilder, parcels);
		}
		if (!lastBlock.isEmpty()) {
			// change parameter mark count to adapt "in" operator values
			condition.setParamMarkCount(lastBlockSize);
			bindInClause(lastBlockSize);
			execute(connectionProvider, queryBuilder, java.util.Collections.singleton(lastBlock));
		}
		return result;
	}
	
	private void bindInClause(int inSize) {
		int[] indexes = new int[inSize];
		for (int i = 0; i < inSize; ) {
			indexes[i] = ++i;
		}
		inOperatorValueIndexes.put(keyColumn, indexes);
		parameterBinders.put(keyColumn, dialect.getColumnBinderRegistry().getBinder(keyColumn));
	}
	
	private void execute(IConnectionProvider connectionProvider,
						 SelectQueryBuilder queryBuilder,
						 Iterable<? extends Iterable<I>> idsParcels) {
		ColumnParamedSelect preparedSelect = new ColumnParamedSelect(queryBuilder.toSQL(), inOperatorValueIndexes, parameterBinders, selectParameterBinders);
		ReadOperation<Column> columnReadOperation = new ReadOperation<>(preparedSelect, connectionProvider);
		for (Iterable<I> parcel : idsParcels) {
			execute(columnReadOperation, parcel);
		}
	}
	
	private void execute(ReadOperation<Column> operation, Iterable<I> ids) {
		try (ReadOperation<Column> closeableOperation = operation) {
			operation.setValue(keyColumn, ids);
			ResultSet resultSet = closeableOperation.execute();
			RowIterator rowIterator = new RowIterator(resultSet, ((ColumnParamedSelect) closeableOperation.getSqlStatement()).getSelectParameterBinders());
			while (rowIterator.hasNext()) {
				result.add(transform(rowIterator));
			}
		} catch (Exception e) {
			throw Exceptions.asRuntimeException(e);
		}
	}
	
	private T transform(RowIterator rowIterator) {
		Row row = rowIterator.next();
		// get entity from main mapping stategy
		T t = joinedStrategySelect.giveRoot().transform(row);
		// complete entity load with complementary mapping strategy
		for (StrategyJoin strategyJoin : joinedStrategySelect.getStrategies()) {
			Object transform = strategyJoin.getStrategy().transform(row);
//				classMappingStrategy.getDefaultMappingStrategy().getRowTransformer().applyRowToBean(row, t);
		}
		return t;
	}
	
	private static class DynamicInClause implements CharSequence {
		
		private String dynamicIn;
		
		public DynamicInClause setParamMarkCount(int markCount) {
			StringAppender result = new StringAppender(10 + markCount * SQL_PARAMETER_MARK_1.length());
			Strings.repeat(result.getAppender(), markCount, SQL_PARAMETER_MARK_1, SQL_PARAMETER_MARK_100, SQL_PARAMETER_MARK_10);
			result.cutTail(2);
			result.wrap("in (", ")");
			dynamicIn = result.toString();
			return this;
		}
		
		@Override
		public int length() {
			return dynamicIn.length();
		}
		
		@Override
		public char charAt(int index) {
			return dynamicIn.charAt(index);
		}
		
		@Override
		public CharSequence subSequence(int start, int end) {
			return dynamicIn.subSequence(start, end);
		}
		
		@Override
		public String toString() {
			return dynamicIn;
		}
	}
}
