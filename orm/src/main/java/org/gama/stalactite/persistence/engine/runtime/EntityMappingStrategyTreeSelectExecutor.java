package org.gama.stalactite.persistence.engine.runtime;

import javax.annotation.Nullable;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import com.google.common.annotations.VisibleForTesting;
import org.gama.lang.StringAppender;
import org.gama.lang.bean.Objects;
import org.gama.lang.collection.Collections;
import org.gama.lang.collection.Iterables;
import org.gama.stalactite.persistence.engine.JoinableSelectExecutor;
import org.gama.stalactite.persistence.engine.runtime.load.EntityJoinTree;
import org.gama.stalactite.persistence.engine.runtime.load.EntityJoinTree.EntityInflater;
import org.gama.stalactite.persistence.engine.runtime.load.EntityJoinTree.EntityInflater.EntityMappingStrategyAdapter;
import org.gama.stalactite.persistence.engine.runtime.load.EntityJoinTree.EntityMerger.EntityMergerAdapter;
import org.gama.stalactite.persistence.engine.runtime.load.EntityJoinTree.JoinType;
import org.gama.stalactite.persistence.engine.runtime.load.EntityTreeInflater;
import org.gama.stalactite.persistence.engine.runtime.load.EntityTreeQueryBuilder;
import org.gama.stalactite.persistence.engine.runtime.load.EntityTreeQueryBuilder.EntityTreeQuery;
import org.gama.stalactite.persistence.mapping.ClassMappingStrategy;
import org.gama.stalactite.persistence.mapping.IEntityMappingStrategy;
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
import org.gama.stalactite.sql.ConnectionProvider;
import org.gama.stalactite.sql.SimpleConnectionProvider;
import org.gama.stalactite.sql.binder.ParameterBinder;
import org.gama.stalactite.sql.binder.ParameterBinderIndex;
import org.gama.stalactite.sql.binder.ParameterBinderIndex.ParameterBinderIndexFromMap;
import org.gama.stalactite.sql.dml.ReadOperation;
import org.gama.stalactite.sql.result.Row;

/**
 * Class aimed at executing a SQL select statement from multiple joined {@link ClassMappingStrategy}.
 * Based on {@link EntityJoinTree} for storing the joins structure and {@link EntityTreeInflater} for building the entities from
 * the {@link ResultSet}.
 * 
 * @author Guillaume Mary
 */
public class EntityMappingStrategyTreeSelectExecutor<C, I, T extends Table> extends SelectExecutor<C, I, T> implements JoinableSelectExecutor {
	
	/** The surrogate for joining the strategies, will help to build the SQL */
	private final EntityJoinTree<C, I> entityJoinTree;
	private final ParameterBinderIndex<Column, ParameterBinder> parameterBinderProvider;
	private final int blockSize;
	
	private final PrimaryKey<Table> primaryKey;
	private final WhereClauseDMLNameProvider whereClauseDMLNameProvider;
	
	public EntityMappingStrategyTreeSelectExecutor(IEntityMappingStrategy<C, I, T> classMappingStrategy,
												   Dialect dialect,
												   ConnectionProvider connectionProvider) {
		super(classMappingStrategy, connectionProvider, dialect.getDmlGenerator(), dialect.getInOperatorMaxSize());
		this.parameterBinderProvider = dialect.getColumnBinderRegistry();
		this.entityJoinTree = new EntityJoinTree<>(new EntityMappingStrategyAdapter<>(classMappingStrategy), classMappingStrategy.getTargetTable());
		this.blockSize = dialect.getInOperatorMaxSize();
		this.primaryKey = classMappingStrategy.getTargetTable().getPrimaryKey();
		// NB: in the condition, table and columns are from the main strategy, so there's no need to use aliases
		this.whereClauseDMLNameProvider = new WhereClauseDMLNameProvider(classMappingStrategy.getTargetTable(), classMappingStrategy.getTargetTable().getAbsoluteName());
	}
	
	public EntityJoinTree<C, I> getEntityJoinTree() {
		return entityJoinTree;
	}
	
	public ParameterBinderIndex<Column, ParameterBinder> getParameterBinderProvider() {
		return parameterBinderProvider;
	}
	
	/**
	 * Adds an inner join to this executor.
	 * Shorcut for {@link EntityJoinTree#addRelationJoin(String, EntityInflater, Column, Column, String, JoinType, BeanRelationFixer)}
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
	public <U, T1 extends Table<T1>, T2 extends Table<T2>, ID> String addRelation(
			String leftStrategyName,
			IEntityMappingStrategy<U, ID, T2> strategy,
			BeanRelationFixer beanRelationFixer,
			Column<T1, ID> leftJoinColumn,
			Column<T2, ID> rightJoinColumn) {
		// we outer join nullable columns
		boolean isOuterJoin = rightJoinColumn.isNullable();
		return addRelation(leftStrategyName, strategy, beanRelationFixer, leftJoinColumn, rightJoinColumn, isOuterJoin);
	}

	/**
	 * Adds a join to this executor.
	 * Shorcut for {@link EntityJoinTree#addRelationJoin(String, EntityInflater, Column, Column, String, JoinType, BeanRelationFixer)}
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
	@Override
	public <U, T1 extends Table<T1>, T2 extends Table<T2>, ID> String addRelation(
			String leftStrategyName,
			IEntityMappingStrategy<U, ID, T2> strategy,
			BeanRelationFixer beanRelationFixer,
			Column<T1, ID> leftJoinColumn,
			Column<T2, ID> rightJoinColumn,
			boolean isOuterJoin) {
		return entityJoinTree.addRelationJoin(leftStrategyName, new EntityMappingStrategyAdapter<>(strategy), leftJoinColumn, rightJoinColumn,
				null, isOuterJoin ? JoinType.OUTER : JoinType.INNER, beanRelationFixer);
	}
	
	/**
	 * Adds a join which {@link Column}s will be added to final select. Data retrieved by those columns will be populated onto final entity
	 * 
	 * @param leftStrategyName join node name onto which join must be added
	 * @param strategy strategy used to apply data onto final bean
	 * @param leftJoinColumn left join column, expected to be one of node table
	 * @param rightJoinColumn right join column, expected to be one of strategy table
	 * @param <U> entity type
	 * @param <T1> left table type
	 * @param <T2> right table type
	 * @param <ID> entity identifier type
	 * @return name of created join node (may be used by caller to add some more join on it)
	 */
	@Override
	public <U, T1 extends Table<T1>, T2 extends Table<T2>, ID> String addComplementaryJoin(
			String leftStrategyName,
			IEntityMappingStrategy<U, ID, T2> strategy,
			Column<T1, ID> leftJoinColumn,
			Column<T2, ID> rightJoinColumn) {
		return entityJoinTree.addMergeJoin(leftStrategyName, new EntityMergerAdapter<>(strategy), leftJoinColumn, rightJoinColumn);
	}
	
	@Override
	public List<C> select(Iterable<I> ids) {
		// cutting ids into pieces, adjusting expected result size
		List<List<I>> parcels = Collections.parcel(ids, blockSize);
		List<C> result = new ArrayList<>(parcels.size() * blockSize);
		if (!parcels.isEmpty()) {
			EntityTreeQuery<C> entityTreeQuery = new EntityTreeQueryBuilder<>(this.entityJoinTree).buildSelectQuery(this.parameterBinderProvider);
			Query query = entityTreeQuery.getQuery();
			
			// Creation of the where clause: we use a dynamic "in" operator clause to avoid multiple QueryBuilder instanciation
			DMLGenerator dmlGenerator = new DMLGenerator(parameterBinderProvider, new NoopSorter(), whereClauseDMLNameProvider);
			DDLAppender identifierCriteria = new JoinDDLAppender(whereClauseDMLNameProvider);
			query.getWhere().and(identifierCriteria);
			
			List<I> lastBlock = Iterables.last(parcels, java.util.Collections.emptyList());
			// keep only full blocks to run them on the fully filled "in" operator
			int lastBlockSize = lastBlock.size();
			if (lastBlockSize != blockSize) {
				parcels = Collections.cutTail(parcels);
			}
			
			SQLQueryBuilder sqlQueryBuilder = new SQLQueryBuilder(query);
			// Be aware that this executor is made to use same Connection to execute next SQL orders in same transaction
			InternalExecutor executor = newInternalExecutor(entityTreeQuery);
			if (!parcels.isEmpty()) {
				// change parameter mark count to adapt "in" operator values
				ParameterizedWhere tableParameterizedWhere = dmlGenerator.appendTupledWhere(identifierCriteria, primaryKey.getColumns(), blockSize);
				result.addAll(executor.execute(sqlQueryBuilder.toSQL(), parcels, tableParameterizedWhere.getColumnToIndex()));
			}
			if (!lastBlock.isEmpty()) {
				// change parameter mark count to adapt "in" operator values, we must clear previous where clause
				identifierCriteria.getAppender().setLength(0);
				ParameterizedWhere tableParameterizedWhere = dmlGenerator.appendTupledWhere(identifierCriteria, primaryKey.getColumns(), lastBlock.size());
				result.addAll(executor.execute(sqlQueryBuilder.toSQL(), java.util.Collections.singleton(lastBlock), tableParameterizedWhere.getColumnToIndex()));
			}
		}
		return result;
	}
	
	@VisibleForTesting
	InternalExecutor newInternalExecutor(EntityTreeQuery<C> entityTreeQuery) {
		return new InternalExecutor(entityTreeQuery,
				// NB : this instance is reused so we must ensure that the same Connection is used for all operations
				new SimpleConnectionProvider(getConnectionProvider().getCurrentConnection()));
	}
	
	/**
	 * Small class that focuses on operation execution and entity loading.
	 * Kind of method group serving same purpose, made non static for simplicity.
	 */
	@VisibleForTesting
	class InternalExecutor {
		
		// We store information to make this instance reusable with different parameters to execute(..) 
		
		private EntityTreeInflater<C> entityTreeInflater;
		private Map<String, ParameterBinder> selectParameterBinders;
		private SelectExecutor.InternalExecutor executor;
		private ConnectionProvider connectionProvider;
		
		@VisibleForTesting
		InternalExecutor(EntityTreeQuery<C> entityTreeQuery, ConnectionProvider connectionProvider) {
			this.entityTreeInflater = entityTreeQuery.getInflater();
			this.selectParameterBinders = entityTreeQuery.getSelectParameterBinders();
			this.connectionProvider = connectionProvider;
			this.executor = new SelectExecutor<C, I, Table>.InternalExecutor() {
				@Override
				protected List<C> transform(Iterator<Row> rowIterator, int resultSize) {
					return entityTreeInflater.transform(() -> rowIterator, resultSize);
				}
			};
		}
		
		@VisibleForTesting
		List<C> execute(String sql, Collection<? extends List<I>> idsParcels, Map<Column<T, Object>, int[]> inOperatorValueIndexes) {
			// binders must be exactly the ones necessary to the request, else an IllegalArgumentException is thrown at execution time
			// so we have to extract them from what is in the request : only primary key columns are parameterized 
			Map<Column<T, Object>, ParameterBinder> primaryKeyBinders = Iterables.map(getMappingStrategy().getTargetTable().getPrimaryKey().getColumns(),
					Function.identity(), parameterBinderProvider::getBinder);
			ColumnParameterizedSelect<T> preparedSelect = new ColumnParameterizedSelect<>(
					sql,
					inOperatorValueIndexes,
					new ParameterBinderIndexFromMap<>(primaryKeyBinders),
					selectParameterBinders);
			List<C> result = new ArrayList<>(idsParcels.size() * blockSize);
			try (ReadOperation<Column<T, Object>> columnReadOperation = new ReadOperation<>(preparedSelect, connectionProvider)) {
				for (List<I> parcel : idsParcels) {
					result.addAll(executor.execute(columnReadOperation, parcel));
				}
			}
			return result;
		}
	}
	
	private static class WhereClauseDMLNameProvider extends DMLNameProvider {
		
		private final Table whereTable;
		private final String whereTableAlias;
		
		private WhereClauseDMLNameProvider(Table whereTable, @Nullable String whereTableAlias) {
			// we don't care about the aliases because we redefine our way of getting them, see #getAlias
			super(java.util.Collections.emptyMap());
			this.whereTable = whereTable;
			this.whereTableAlias = whereTableAlias;
		}
		
		/** Overriden to get alias of the root and only for it, throws exception it given table is not where table one */
		@Override
		public String getAlias(Table table) {
			if (table == whereTable) {
				return Objects.preventNull(whereTableAlias, table.getName());
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
