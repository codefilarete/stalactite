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
import org.gama.sql.ConnectionProvider;
import org.gama.sql.SimpleConnectionProvider;
import org.gama.sql.binder.ParameterBinder;
import org.gama.sql.binder.ParameterBinderIndex;
import org.gama.sql.dml.ReadOperation;
import org.gama.sql.result.Row;
import org.gama.stalactite.persistence.engine.BeanRelationFixer;
import org.gama.stalactite.persistence.engine.SelectExecutor;
import org.gama.stalactite.persistence.engine.cascade.JoinedStrategiesSelect.StrategyJoins;
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
import org.gama.stalactite.query.builder.SQLQueryBuilder;
import org.gama.stalactite.query.model.Query;

import static org.gama.stalactite.persistence.engine.cascade.JoinedStrategiesSelect.FIRST_STRATEGY_NAME;

/**
 * Class aimed at executing a SQL select statement from multiple joined {@link ClassMappingStrategy}.
 * Based on {@link JoinedStrategiesSelect} for storing the joins structure and {@link StrategyJoinsRowTransformer} for building the entities from
 * the {@link ResultSet}.
 * 
 * @author Guillaume Mary
 */
public class JoinedStrategiesSelectExecutor<C, I, T extends Table> extends SelectExecutor<C, I, T>  {
	
	/** The surrogate for joining the strategies, will help to build the SQL */
	private final JoinedStrategiesSelect<C, I, T> joinedStrategiesSelect;
	private final ParameterBinderIndex<Column, ParameterBinder> parameterBinderProvider;
	private final int blockSize;
	
	private final PrimaryKey<Table> primaryKey;
	
	public JoinedStrategiesSelectExecutor(ClassMappingStrategy<C, I, T> classMappingStrategy, Dialect dialect, ConnectionProvider connectionProvider) {
		super(classMappingStrategy, connectionProvider, dialect.getDmlGenerator(), dialect.getInOperatorMaxSize());
		this.parameterBinderProvider = dialect.getColumnBinderRegistry();
		this.joinedStrategiesSelect = new JoinedStrategiesSelect<>(classMappingStrategy, this.parameterBinderProvider);
		this.blockSize = dialect.getInOperatorMaxSize();
		this.primaryKey = classMappingStrategy.getTargetTable().getPrimaryKey();
	}
	
	public JoinedStrategiesSelect<C, I, T> getJoinedStrategiesSelect() {
		return joinedStrategiesSelect;
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
	public <U, T1 extends Table<T1>, T2 extends Table<T2>, ID> String addComplementaryTable(
			String leftStrategyName,
			ClassMappingStrategy<U, ID, T2> strategy,
			BeanRelationFixer beanRelationFixer,
			Column<T1, ID> leftJoinColumn,
			Column<T2, ID> rightJoinColumn) {
		// we outer join nullable columns
		boolean isOuterJoin = rightJoinColumn.isNullable();
		return addComplementaryTable(leftStrategyName, strategy, beanRelationFixer, leftJoinColumn, rightJoinColumn, isOuterJoin);
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
	public <U, T1 extends Table<T1>, T2 extends Table<T2>, ID> String addComplementaryTable(
			String leftStrategyName,
			ClassMappingStrategy<U, ID, T2> strategy,
			BeanRelationFixer beanRelationFixer,
			Column<T1, ID> leftJoinColumn,
			Column<T2, ID> rightJoinColumn,
			boolean isOuterJoin) {
		return joinedStrategiesSelect.add(leftStrategyName, strategy, leftJoinColumn, rightJoinColumn, isOuterJoin, beanRelationFixer);
	}
	
	@Override
	public List<C> select(Iterable<I> ids) {
		// cutting ids into pieces, adjusting expected result size
		List<List<I>> parcels = Collections.parcel(ids, blockSize);
		List<C> result = new ArrayList<>(parcels.size() * blockSize);
		if (!parcels.isEmpty()) {
			Query query = joinedStrategiesSelect.buildSelectQuery();
			
			// Creation of the where clause: we use a dynamic "in" operator clause to avoid multiple QueryBuilder instanciation
			// NB: in the condition, table and columns are from the main strategy, so there's no need to use aliases
			DMLNameProvider dmlNameProvider = new WhereClauseDMLNameProvider(joinedStrategiesSelect);
			DMLGenerator dmlGenerator = new DMLGenerator(parameterBinderProvider, new NoopSorter(), dmlNameProvider);
			DDLAppender identifierCriteria = new JoinDDLAppender(dmlNameProvider);
			query.getWhere().and(identifierCriteria);
			
			// We ensure that the same Connection is used for all operations
			ConnectionProvider localConnectionProvider = new SimpleConnectionProvider(getConnectionProvider().getCurrentConnection());
			List<I> lastBlock = Iterables.last(parcels, java.util.Collections.emptyList());
			// keep only full blocks to run them on the fully filled "in" operator
			int lastBlockSize = lastBlock.size();
			if (lastBlockSize != blockSize) {
				parcels = Collections.cutTail(parcels);
			}
			
			SQLQueryBuilder SQLQueryBuilder = new SQLQueryBuilder(query);
			if (!parcels.isEmpty()) {
				// change parameter mark count to adapt "in" operator values
				ParameterizedWhere tableParameterizedWhere = dmlGenerator.appendTupledWhere(identifierCriteria, primaryKey.getColumns(), blockSize);
				result.addAll(execute(localConnectionProvider, SQLQueryBuilder.toSQL(), parcels, tableParameterizedWhere.getColumnToIndex()));
			}
			if (!lastBlock.isEmpty()) {
				// change parameter mark count to adapt "in" operator values, we must clear previous where clause
				identifierCriteria.getAppender().setLength(0);
				ParameterizedWhere tableParameterizedWhere = dmlGenerator.appendTupledWhere(identifierCriteria, primaryKey.getColumns(), lastBlock.size());
				result.addAll(execute(localConnectionProvider, SQLQueryBuilder.toSQL(), java.util.Collections.singleton(lastBlock), tableParameterizedWhere.getColumnToIndex()));
			}
		}
		return result;
	}
	
	List<C> execute(ConnectionProvider connectionProvider, String sql, Collection<? extends List<I>> idsParcels, Map<Column, int[]> inOperatorValueIndexes) {
		List<C> result = new ArrayList<>(idsParcels.size() * blockSize);
		ColumnParameterizedSelect preparedSelect = new ColumnParameterizedSelect(sql, inOperatorValueIndexes, parameterBinderProvider, joinedStrategiesSelect.getSelectParameterBinders());
		try (ReadOperation<Column<T, Object>> columnReadOperation = new ReadOperation<>(preparedSelect, connectionProvider)) {
			for (List<I> parcel : idsParcels) {
				result.addAll(execute(columnReadOperation, parcel));
			}
		}
		return result;
	}
	
	@Override
	protected List<C> transform(Iterator<Row> rowIterator, int resultSize) {
		StrategyJoinsRowTransformer<C> strategyJoinsRowTransformer = new StrategyJoinsRowTransformer<>(joinedStrategiesSelect.getStrategyJoins(FIRST_STRATEGY_NAME));
		strategyJoinsRowTransformer.setAliases(this.joinedStrategiesSelect.getAliases());
		return strategyJoinsRowTransformer.transform(() -> rowIterator, resultSize);
	}
	
	private static class WhereClauseDMLNameProvider extends DMLNameProvider {
		
		private final JoinedStrategiesSelect joinedStrategiesSelect;
		
		private WhereClauseDMLNameProvider(JoinedStrategiesSelect joinedStrategiesSelect) {
			// we don't care about the aliases because we redefine our way of getting them, see #getAlias
			super(java.util.Collections.emptyMap());
			this.joinedStrategiesSelect = joinedStrategiesSelect;
		}
		
		/** Overriden to get alias from the root {@link StrategyJoins} table alias (if any) */
		@Override
		public String getAlias(Table table) {
			StrategyJoins rootStrategyJoins = joinedStrategiesSelect.getStrategyJoins(FIRST_STRATEGY_NAME);
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
	private static class JoinDDLAppender extends DDLAppender {
		
		/** Made transient to comply with {@link java.io.Serializable} contract of parent class */
		private final transient DMLNameProvider dmlNameProvider;
		
		private JoinDDLAppender(DMLNameProvider dmlNameProvider) {
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
