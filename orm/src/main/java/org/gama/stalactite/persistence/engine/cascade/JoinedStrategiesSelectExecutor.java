package org.gama.stalactite.persistence.engine.cascade;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.gama.lang.StringAppender;
import org.gama.lang.bean.Objects;
import org.gama.lang.collection.Collections;
import org.gama.lang.collection.Iterables;
import org.gama.lang.exception.Exceptions;
import org.gama.sql.ConnectionProvider;
import org.gama.sql.SimpleConnectionProvider;
import org.gama.sql.binder.ParameterBinder;
import org.gama.sql.binder.ParameterBinderIndex;
import org.gama.sql.dml.ReadOperation;
import org.gama.sql.result.Row;
import org.gama.sql.result.RowIterator;
import org.gama.stalactite.persistence.engine.BeanRelationFixer;
import org.gama.stalactite.persistence.engine.cascade.JoinedStrategiesSelect.StrategyJoins;
import org.gama.stalactite.persistence.id.assembly.IdentifierAssembler;
import org.gama.stalactite.persistence.mapping.ClassMappingStrategy;
import org.gama.stalactite.persistence.sql.Dialect;
import org.gama.stalactite.persistence.sql.ddl.DDLAppender;
import org.gama.stalactite.persistence.sql.dml.ColumnParameterizedSelect;
import org.gama.stalactite.persistence.sql.dml.DMLGenerator;
import org.gama.stalactite.persistence.sql.dml.DMLGenerator.NoopSorter;
import org.gama.stalactite.persistence.sql.dml.DMLGenerator.ParameterizedWhere;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.PrimaryKey;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.query.builder.DMLNameProvider;
import org.gama.stalactite.query.builder.QueryBuilder;
import org.gama.stalactite.query.model.Query;

/**
 * Class aimed at executing a SQL select statement from multiple joined {@link ClassMappingStrategy}.
 * Based on {@link JoinedStrategiesSelect} for storing the joins structure and {@link StrategyJoinsRowTransformer} for building the entities from
 * the {@link ResultSet}.
 * 
 * @author Guillaume Mary
 */
public class JoinedStrategiesSelectExecutor<C, I> {
	
	/** The surrogate for joining the strategies, will help to build the SQL */
	private final JoinedStrategiesSelect<C, I, ? extends Table> joinedStrategiesSelect;
	private final ParameterBinderIndex<Column, ParameterBinder> parameterBinderProvider;
	private final int blockSize;
	private final ConnectionProvider connectionProvider;
	
	private final PrimaryKey<Table> primaryKey;
	private final IdentifierAssembler<I> identifierAssembler;
	
	JoinedStrategiesSelectExecutor(ClassMappingStrategy<C, I, ? extends Table> classMappingStrategy, Dialect dialect, ConnectionProvider connectionProvider) {
		this.parameterBinderProvider = dialect.getColumnBinderRegistry();
		this.joinedStrategiesSelect = new JoinedStrategiesSelect<>(classMappingStrategy, this.parameterBinderProvider);
		this.connectionProvider = connectionProvider;
		this.blockSize = dialect.getInOperatorMaxSize();
		this.primaryKey = classMappingStrategy.getTargetTable().getPrimaryKey();
		this.identifierAssembler = classMappingStrategy.getIdMappingStrategy().getIdentifierAssembler();
	}
	
	public <T extends Table<T>> JoinedStrategiesSelect<C, I, T> getJoinedStrategiesSelect() {
		return (JoinedStrategiesSelect<C, I, T>) joinedStrategiesSelect;
	}
	
	public ParameterBinderIndex<Column, ParameterBinder> getParameterBinderProvider() {
		return parameterBinderProvider;
	}
	
	/**
	 * Adds an inner join to this executor.
	 * Shorcu for {@link JoinedStrategiesSelect#add(String, ClassMappingStrategy, Column, Column, boolean, BeanRelationFixer)}
	 *
	 * @param leftStrategyName the name of a (previously) registered join. {@code leftJoinColumn} must be a {@link Column} of its left {@link Table}
	 * @param strategy the strategy of the mapped bean. Used to give {@link Column}s and {@link org.gama.stalactite.persistence.mapping.IRowTransformer}
	 * @param leftJoinColumn the {@link Column} (of previous strategy left table) to be joined with {@code rightJoinColumn}
	 * @param rightJoinColumn the {@link Column} (of the strategy table) to be joined with {@code leftJoinColumn}
	 * @param beanRelationFixer a function to fullfill relation between 2 strategies beans
	 * @param <U> type of bean mapped by the given strategy
	 * @param <T1> joined left table
	 * @param <T2> joined right table
	 * @param <ID> type of joined values
	 * @return the name of the created join, to be used as a key for other joins (through this method {@code leftStrategyName} argument)
	 */
	public <U, T1 extends Table<T1>, T2 extends Table<T2>, ID> String addComplementaryTables(
			String leftStrategyName,
			ClassMappingStrategy<U, ID, T2> strategy,
			BeanRelationFixer beanRelationFixer,
			Column<T1, ID> leftJoinColumn,
			Column<T2, ID> rightJoinColumn) {
		// we outer join nullable columns
		boolean isOuterJoin = rightJoinColumn.isNullable();
		return addComplementaryTables(leftStrategyName, strategy, beanRelationFixer, leftJoinColumn, rightJoinColumn, isOuterJoin);
	}
	
	/**
	 * Adds a join to this executor.
	 * Shorcu for {@link JoinedStrategiesSelect#add(String, ClassMappingStrategy, Column, Column, boolean, BeanRelationFixer)}
	 *
	 * @param leftStrategyName the name of a (previously) registered join. {@code leftJoinColumn} must be a {@link Column} of its left {@link Table}
	 * @param strategy the strategy of the mapped bean. Used to give {@link Column}s and {@link org.gama.stalactite.persistence.mapping.IRowTransformer}
	 * @param leftJoinColumn the {@link Column} (of previous strategy left table) to be joined with {@code rightJoinColumn}
	 * @param rightJoinColumn the {@link Column} (of the strategy table) to be joined with {@code leftJoinColumn}
	 * @param isOuterJoin says wether or not the join must be open
	 * @param beanRelationFixer a function to fullfill relation between 2 strategies beans
	 * @param <U> type of bean mapped by the given strategy
	 * @param <T1> joined left table
	 * @param <T2> joined right table
	 * @param <ID> type of joined values
	 * @return the name of the created join, to be used as a key for other joins (through this method {@code leftStrategyName} argument)
	 */
	public <U, T1 extends Table<T1>, T2 extends Table<T2>, ID> String addComplementaryTables(
			String leftStrategyName,
			ClassMappingStrategy<U, ID, T2> strategy,
			BeanRelationFixer beanRelationFixer,
			Column<T1, ID> leftJoinColumn,
			Column<T2, ID> rightJoinColumn,
			boolean isOuterJoin) {
		return joinedStrategiesSelect.add(leftStrategyName, strategy, leftJoinColumn, rightJoinColumn, isOuterJoin, beanRelationFixer);
	}
	
	public List<C> select(Collection<I> ids) {
		// cutting ids into pieces, adjusting expected result size
		List<List<I>> parcels = Collections.parcel(ids, blockSize);
		List<C> result = new ArrayList<>(parcels.size() * blockSize);
		if (!parcels.isEmpty()) {
			Query query = joinedStrategiesSelect.buildSelectQuery();
			
			// Creation of the where clause: we use a dynamic "in" operator clause to avoid multiple QueryBuilder instanciation
			// NB: in the condition, table and columns are from the main strategy, so there's no need to use aliases
			DMLNameProvider dmlNameProvider = new WhereClauseDMLNameProvider();
			DMLGenerator dmlGenerator = new DMLGenerator(parameterBinderProvider, new NoopSorter(), dmlNameProvider);
			DDLAppender whereClause = new JoinDMLAppender(dmlNameProvider);
			query.getWhere().and(whereClause);
			
			// We ensure that the same Connection is used for all operations
			ConnectionProvider localConnectionProvider = new SimpleConnectionProvider(connectionProvider.getCurrentConnection());
			List<I> lastBlock = Iterables.last(parcels, java.util.Collections.emptyList());
			// keep only full blocks to run them on the fully filled "in" operator
			int lastBlockSize = lastBlock.size();
			if (lastBlockSize != blockSize) {
				parcels = Collections.cutTail(parcels);
			}
			
			QueryBuilder queryBuilder = new QueryBuilder(query);
			if (!parcels.isEmpty()) {
				// change parameter mark count to adapt "in" operator values
				ParameterizedWhere tableParameterizedWhere = dmlGenerator.appendTupledWhere(whereClause, primaryKey.getColumns(), blockSize);
				result.addAll(execute(localConnectionProvider, queryBuilder.toSQL(), parcels, tableParameterizedWhere.getColumnToIndex()));
			}
			if (!lastBlock.isEmpty()) {
				// change parameter mark count to adapt "in" operator values, we must clear previous where clause
				whereClause.getAppender().setLength(0);
				ParameterizedWhere tableParameterizedWhere = dmlGenerator.appendTupledWhere(whereClause, primaryKey.getColumns(), lastBlock.size());
				result.addAll(execute(localConnectionProvider, queryBuilder.toSQL(), java.util.Collections.singleton(lastBlock), tableParameterizedWhere.getColumnToIndex()));
			}
		}
		return result;
	}
	
	List<C> execute(ConnectionProvider connectionProvider, String sql, Collection<? extends List<I>> idsParcels, Map<Column, int[]> inOperatorValueIndexes) {
		List<C> result = new ArrayList<>(idsParcels.size() * blockSize);
		ColumnParameterizedSelect preparedSelect = new ColumnParameterizedSelect(sql, inOperatorValueIndexes, parameterBinderProvider, joinedStrategiesSelect.getSelectParameterBinders());
		try (ReadOperation<Column<Table, Object>> columnReadOperation = new ReadOperation<>(preparedSelect, connectionProvider)) {
			for (List<I> parcel : idsParcels) {
				result.addAll(execute(columnReadOperation, parcel));
			}
		}
		return result;
	}
	
	List<C> execute(ReadOperation<Column<Table, Object>> operation, List<I> ids) {
		Map<Column<Table, Object>, Object> primaryKeyValues = identifierAssembler.getColumnValues(ids);
		try (ReadOperation<Column<Table, Object>> closeableOperation = operation) {
			closeableOperation.setValues(primaryKeyValues);
			ResultSet resultSet = closeableOperation.execute();
			// NB: we give the same ParametersBinders of those given at ColumnParamedSelect since the row iterator is expected to read column from it
			RowIterator rowIterator = new RowIterator(resultSet, ((ColumnParameterizedSelect) closeableOperation.getSqlStatement()).getSelectParameterBinders());
			return transform(rowIterator);
		} catch (Exception e) {
			throw Exceptions.asRuntimeException(e);
		}
	}
	
	List<C> transform(Iterator<Row> rowIterator) {
		StrategyJoinsRowTransformer<C> strategyJoinsRowTransformer = new StrategyJoinsRowTransformer<>(
				joinedStrategiesSelect.getStrategyJoins(JoinedStrategiesSelect.FIRST_STRATEGY_NAME));
		
		strategyJoinsRowTransformer.setAliases(this.joinedStrategiesSelect.getAliases());
		return strategyJoinsRowTransformer.transform(() -> rowIterator);
	}
	
	private class WhereClauseDMLNameProvider extends DMLNameProvider {
		public WhereClauseDMLNameProvider() {
			// we don't care about the aliases because we redefine our way of getting them, see #getAlias
			super(java.util.Collections.emptyMap());
		}
		
		/** Overriden to get alias from the root {@link StrategyJoins} table alias (if any) */
		@Override
		public String getAlias(Table table) {
			StrategyJoins rootStrategyJoins = JoinedStrategiesSelectExecutor.this.joinedStrategiesSelect.getStrategyJoins(JoinedStrategiesSelect.FIRST_STRATEGY_NAME);
			if (table == rootStrategyJoins.getTable()) {
				return Objects.preventNull(rootStrategyJoins.getTableAlias(), table.getName());
			} else {
				// anti unexpected usage
				throw new IllegalArgumentException("Table " + table.getName() + " is not expected to be used in the where clause");
			}
		}
	}
	
	/**
	 * A dedicated {@link DDLAppender} for joins : it prefixes columns with their table alias (or name)
	 */
	private static class JoinDMLAppender extends DDLAppender {
		
		private final DMLNameProvider dmlNameProvider;
		
		public JoinDMLAppender(DMLNameProvider dmlNameProvider) {
			super(dmlNameProvider);
			this.dmlNameProvider = dmlNameProvider;
		}
		
		/** Overriden to change the way {@link Column}s are appended : their table prefix are added */
		@Override
		public StringAppender cat(Object o) {
			if (o instanceof Column) {
				return super.cat(dmlNameProvider.getName((Column) o));
			} else {
				return super.cat(o);
			}
		}
	}
	
}
