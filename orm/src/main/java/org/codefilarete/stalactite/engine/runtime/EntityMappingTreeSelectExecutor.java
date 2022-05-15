package org.codefilarete.stalactite.engine.runtime;

import javax.annotation.Nullable;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.codefilarete.stalactite.engine.JoinableSelectExecutor;
import org.codefilarete.stalactite.engine.configurer.PersisterBuilderContext;
import org.codefilarete.stalactite.engine.configurer.PersisterBuilderImpl.BuildLifeCycleListener;
import org.codefilarete.stalactite.engine.runtime.load.EntityInflater;
import org.codefilarete.stalactite.engine.runtime.load.EntityInflater.EntityMappingAdapter;
import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree;
import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree.JoinType;
import org.codefilarete.stalactite.engine.runtime.load.EntityMerger.EntityMergerAdapter;
import org.codefilarete.stalactite.engine.runtime.load.EntityTreeInflater;
import org.codefilarete.stalactite.engine.runtime.load.EntityTreeQueryBuilder;
import org.codefilarete.stalactite.engine.runtime.load.EntityTreeQueryBuilder.EntityTreeQuery;
import org.codefilarete.stalactite.mapping.ClassMapping;
import org.codefilarete.stalactite.mapping.EntityMapping;
import org.codefilarete.stalactite.mapping.RowTransformer;
import org.codefilarete.stalactite.mapping.id.assembly.IdentifierAssembler;
import org.codefilarete.stalactite.query.builder.DMLNameProvider;
import org.codefilarete.stalactite.query.builder.SQLQueryBuilder;
import org.codefilarete.stalactite.sql.ConnectionProvider;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.SimpleConnectionProvider;
import org.codefilarete.stalactite.sql.ddl.DDLAppender;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.PrimaryKey;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.BeanRelationFixer;
import org.codefilarete.stalactite.sql.result.Row;
import org.codefilarete.stalactite.sql.statement.ColumnParameterizedSelect;
import org.codefilarete.stalactite.sql.statement.DMLGenerator;
import org.codefilarete.stalactite.sql.statement.DMLGenerator.NoopSorter;
import org.codefilarete.stalactite.sql.statement.DMLGenerator.ParameterizedWhere;
import org.codefilarete.stalactite.sql.statement.ReadOperation;
import org.codefilarete.stalactite.sql.statement.SQLOperation.SQLOperationListener;
import org.codefilarete.stalactite.sql.statement.binder.ParameterBinder;
import org.codefilarete.stalactite.sql.statement.binder.ParameterBinderIndex;
import org.codefilarete.stalactite.sql.statement.binder.ParameterBinderIndex.ParameterBinderIndexFromMap;
import org.codefilarete.tool.StringAppender;
import org.codefilarete.tool.VisibleForTesting;
import org.codefilarete.tool.bean.Objects;
import org.codefilarete.tool.collection.Collections;
import org.codefilarete.tool.collection.Iterables;

/**
 * Class aimed at executing a SQL select statement from multiple joined {@link ClassMapping}.
 * Based on {@link EntityJoinTree} for storing the joins structure and {@link EntityTreeInflater} for building the entities from
 * the {@link ResultSet}.
 * 
 * @author Guillaume Mary
 */
public class EntityMappingTreeSelectExecutor<C, I, T extends Table> implements org.codefilarete.stalactite.engine.SelectExecutor<C, I>, JoinableSelectExecutor {
	
	/** The surrogate for joining the strategies, will help to build the SQL */
	private final EntityJoinTree<C, I> entityJoinTree;
	private final ParameterBinderIndex<Column, ParameterBinder> parameterBinderProvider;
	private final int blockSize;
	
	private final PrimaryKey<T> primaryKey;
	private final WhereClauseDMLNameProvider whereClauseDMLNameProvider;
	private final ConnectionProvider connectionProvider;
	private final IdentifierAssembler<I> identifierAssembler;
	
	protected SQLOperationListener<Column<T, Object>> operationListener;
	
	private final DMLGenerator dmlGenerator;
	private String rawQuery;
	private EntityTreeQuery<C> entityTreeQuery;
	private ParameterBinderIndexFromMap<Column<T, Object>, ParameterBinder> parameterBinderForPKInSelect;
	
	public EntityMappingTreeSelectExecutor(EntityMapping<C, I, T> entityMapping,
										   Dialect dialect,
										   ConnectionProvider connectionProvider) {
		this.parameterBinderProvider = dialect.getColumnBinderRegistry();
		this.identifierAssembler = entityMapping.getIdMapping().getIdentifierAssembler();
		this.entityJoinTree = new EntityJoinTree<>(new EntityMappingAdapter<>(entityMapping), entityMapping.getTargetTable());
		this.blockSize = dialect.getInOperatorMaxSize();
		this.primaryKey = entityMapping.getTargetTable().getPrimaryKey();
		// NB: in the condition, table and columns are from the main strategy, so there's no need to use aliases
		this.whereClauseDMLNameProvider = new WhereClauseDMLNameProvider(entityMapping.getTargetTable(), entityMapping.getTargetTable().getAbsoluteName());
		this.connectionProvider = connectionProvider;
		
		this.dmlGenerator = new DMLGenerator(parameterBinderProvider, new NoopSorter(), whereClauseDMLNameProvider);
		
		parameterBinderForPKInSelect = new ParameterBinderIndexFromMap<>(Iterables.map(primaryKey.getColumns(), Function.identity(), parameterBinderProvider::getBinder));
		
		PersisterBuilderContext currentBuilderContext = PersisterBuilderContext.CURRENT.get();
		currentBuilderContext.addBuildLifeCycleListener(new BuildLifeCycleListener() {
			@Override
			public void afterAllBuild() {
				prepareQuery();
			}
		});
	}
	
	@VisibleForTesting
	void prepareQuery() {
		this.entityTreeQuery = new EntityTreeQueryBuilder<>(this.entityJoinTree, parameterBinderProvider).buildSelectQuery();
		this.rawQuery = new SQLQueryBuilder(entityTreeQuery.getQuery()).toSQL();
	}
	
	public EntityJoinTree<C, I> getEntityJoinTree() {
		return entityJoinTree;
	}
	
	public ParameterBinderIndex<Column, ParameterBinder> getParameterBinderProvider() {
		return parameterBinderProvider;
	}
	
	public void setOperationListener(SQLOperationListener<Column<T, Object>> operationListener) {
		this.operationListener = operationListener;
	}
	
	/**
	 * Adds an inner join to this executor.
	 * Shorcut for {@link EntityJoinTree#addRelationJoin(String, EntityInflater, Column, Column, String, JoinType, BeanRelationFixer, java.util.Set)}
	 *
	 * @param leftStrategyName the name of a (previously) registered join. {@code leftJoinColumn} must be a {@link Column} of its left {@link Table}
	 * @param strategy the strategy of the mapped bean. Used to give {@link Column}s and {@link RowTransformer}
	 * @param leftJoinColumn the {@link Column} (of previous strategy left table) to be joined with {@code rightJoinColumn}
	 * @param rightJoinColumn the {@link Column} (of the strategy table) to be joined with {@code leftJoinColumn}
	 * @param beanRelationFixer a function to fulfill relation between 2 strategies beans
	 * @param <U> type of bean mapped by the given strategy
	 * @param <T1> joined left table
	 * @param <T2> joined right table
	 * @param <ID> type of joined values
	 * @return the name of the created join, to be used as a key for other joins (through this method {@code leftStrategyName} argument)
	 */
	public <U, T1 extends Table<T1>, T2 extends Table<T2>, ID> String addRelation(
			String leftStrategyName,
			EntityMapping<U, ID, T2> strategy,
			BeanRelationFixer beanRelationFixer,
			Column<T1, ID> leftJoinColumn,
			Column<T2, ID> rightJoinColumn) {
		// we outer join nullable columns
		boolean isOuterJoin = rightJoinColumn.isNullable();
		return addRelation(leftStrategyName, strategy, beanRelationFixer, leftJoinColumn, rightJoinColumn, isOuterJoin);
	}

	/**
	 * Adds a join to this executor.
	 * Shorcut for {@link EntityJoinTree#addRelationJoin(String, EntityInflater, Column, Column, String, JoinType, BeanRelationFixer, java.util.Set)}
	 *
	 * @param leftStrategyName the name of a (previously) registered join. {@code leftJoinColumn} must be a {@link Column} of its left {@link Table}
	 * @param strategy the strategy of the mapped bean. Used to give {@link Column}s and {@link RowTransformer}
	 * @param leftJoinColumn the {@link Column} (of previous strategy left table) to be joined with {@code rightJoinColumn}
	 * @param rightJoinColumn the {@link Column} (of the strategy table) to be joined with {@code leftJoinColumn}
	 * @param isOuterJoin says wether or not the join must be open
	 * @param beanRelationFixer a function to fulfill relation between 2 strategies beans
	 * @param <U> type of bean mapped by the given strategy
	 * @param <T1> joined left table
	 * @param <T2> joined right table
	 * @param <ID> type of joined values
	 * @return the name of the created join, to be used as a key for other joins (through this method {@code leftStrategyName} argument)
	 */
	@Override
	public <U, T1 extends Table<T1>, T2 extends Table<T2>, ID> String addRelation(
			String leftStrategyName,
			EntityMapping<U, ID, T2> strategy,
			BeanRelationFixer beanRelationFixer,
			Column<T1, ID> leftJoinColumn,
			Column<T2, ID> rightJoinColumn,
			boolean isOuterJoin) {
		return entityJoinTree.addRelationJoin(leftStrategyName, new EntityMappingAdapter<>(strategy), leftJoinColumn, rightJoinColumn,
											  null, isOuterJoin ? JoinType.OUTER : JoinType.INNER, beanRelationFixer, java.util.Collections.emptySet());
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
	public <U, T1 extends Table<T1>, T2 extends Table<T2>, ID> String addMergeJoin(
			String leftStrategyName,
			EntityMapping<U, ID, T2> strategy,
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
			// Creation of the where clause: we use a dynamic "in" operator clause to avoid multiple QueryBuilder instantiation
			DDLAppender identifierCriteria = new JoinDDLAppender(whereClauseDMLNameProvider);
			
			List<I> lastBlock = Iterables.last(parcels, java.util.Collections.emptyList());
			// keep only full blocks to run them on the fully filled "in" operator
			int lastBlockSize = lastBlock.size();
			if (lastBlockSize != blockSize) {
				parcels = Collections.cutTail(parcels);
			}
			
			// Be aware that this executor is made to use same Connection to execute next SQL orders in same transaction
			InternalExecutor executor = newInternalExecutor(entityTreeQuery);
			if (!parcels.isEmpty()) {
				// change parameter mark count to adapt "in" operator values
				ParameterizedWhere tableParameterizedWhere = dmlGenerator.appendTupledWhere(identifierCriteria, primaryKey.getColumns(), blockSize);
				result.addAll(executor.execute(rawQuery + " where " + identifierCriteria, parcels, tableParameterizedWhere.getColumnToIndex()));
			}
			if (!lastBlock.isEmpty()) {
				// change parameter mark count to adapt "in" operator values, we must clear previous where clause
				identifierCriteria.getAppender().setLength(0);
				ParameterizedWhere tableParameterizedWhere = dmlGenerator.appendTupledWhere(identifierCriteria, primaryKey.getColumns(), lastBlock.size());
				result.addAll(executor.execute(rawQuery + " where " + identifierCriteria, java.util.Collections.singleton(lastBlock), tableParameterizedWhere.getColumnToIndex()));
			}
		}
		return result;
	}
	
	@VisibleForTesting
	InternalExecutor newInternalExecutor(EntityTreeQuery<C> entityTreeQuery) {
		return new InternalExecutor(entityTreeQuery,
									// NB : this instance is reused so we must ensure that the same Connection is used for all operations
									new SimpleConnectionProvider(connectionProvider.giveConnection()));
	}
	
	/**
	 * Small class that focuses on operation execution and entity loading.
	 * Kind of method group serving same purpose, made non-static for simplicity.
	 */
	@VisibleForTesting
	class InternalExecutor {
		
		// We store information to make this instance reusable with different parameters to execute(..) 
		
		private final EntityTreeInflater<C> entityTreeInflater;
		private final Map<String, ParameterBinder> selectParameterBinders;
		private final SelectExecutor.InternalExecutor executor;
		private final ConnectionProvider connectionProvider;
		
		@VisibleForTesting
		InternalExecutor(EntityTreeQuery<C> entityTreeQuery, ConnectionProvider connectionProvider) {
			this.entityTreeInflater = entityTreeQuery.getInflater();
			this.selectParameterBinders = entityTreeQuery.getSelectParameterBinders();
			this.connectionProvider = connectionProvider;
			// we pass null as transformer because we override transform(..) method
			this.executor = new SelectExecutor.InternalExecutor<C, I, T>(identifierAssembler, null) {
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
			ColumnParameterizedSelect<T> preparedSelect = new ColumnParameterizedSelect<>(
					sql,
					inOperatorValueIndexes,
					parameterBinderForPKInSelect,
					selectParameterBinders);
			List<C> result = new ArrayList<>(idsParcels.size() * blockSize);
			try (ReadOperation<Column<T, Object>> columnReadOperation = new ReadOperation<>(preparedSelect, connectionProvider)) {
				columnReadOperation.setListener(EntityMappingTreeSelectExecutor.this.operationListener);
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
		
		/** Overridden to get alias of the root and only for it, throws exception if given table is not where table one */
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
		
		/** Overridden to change the way {@link Column}s are appended : their table prefix are added */
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
