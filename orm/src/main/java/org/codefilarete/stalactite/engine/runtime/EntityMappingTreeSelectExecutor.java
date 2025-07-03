package org.codefilarete.stalactite.engine.runtime;

import javax.annotation.Nullable;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import org.codefilarete.stalactite.engine.configurer.PersisterBuilderContext;
import org.codefilarete.stalactite.engine.configurer.PersisterBuilderImpl.BuildLifeCycleListener;
import org.codefilarete.stalactite.engine.runtime.load.EntityInflater.EntityMappingAdapter;
import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree;
import org.codefilarete.stalactite.engine.runtime.load.EntityTreeInflater;
import org.codefilarete.stalactite.engine.runtime.load.EntityTreeQueryBuilder;
import org.codefilarete.stalactite.engine.runtime.load.EntityTreeQueryBuilder.EntityTreeQuery;
import org.codefilarete.stalactite.mapping.ClassMapping;
import org.codefilarete.stalactite.mapping.EntityMapping;
import org.codefilarete.stalactite.mapping.id.assembly.IdentifierAssembler;
import org.codefilarete.stalactite.query.builder.DMLNameProvider;
import org.codefilarete.stalactite.query.builder.QuerySQLBuilderFactory;
import org.codefilarete.stalactite.query.model.Fromable;
import org.codefilarete.stalactite.query.model.Selectable;
import org.codefilarete.stalactite.sql.ConnectionProvider;
import org.codefilarete.stalactite.sql.DMLNameProviderFactory;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.SimpleConnectionProvider;
import org.codefilarete.stalactite.sql.ddl.DDLAppender;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.PrimaryKey;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.ColumnedRow;
import org.codefilarete.stalactite.sql.statement.ColumnParameterizedSelect;
import org.codefilarete.stalactite.sql.statement.DMLGenerator;
import org.codefilarete.stalactite.sql.statement.DMLGenerator.NoopSorter;
import org.codefilarete.stalactite.sql.statement.DMLGenerator.ParameterizedWhere;
import org.codefilarete.stalactite.sql.statement.ReadOperation;
import org.codefilarete.stalactite.sql.statement.SQLOperation.SQLOperationListener;
import org.codefilarete.stalactite.sql.statement.binder.ColumnBinderRegistry;
import org.codefilarete.stalactite.sql.statement.binder.ParameterBinder;
import org.codefilarete.stalactite.sql.statement.binder.ParameterBinderIndex.ParameterBinderIndexFromMap;
import org.codefilarete.tool.StringAppender;
import org.codefilarete.tool.VisibleForTesting;
import org.codefilarete.tool.bean.Objects;
import org.codefilarete.tool.collection.Collections;
import org.codefilarete.tool.collection.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class aimed at executing a SQL select statement from multiple joined {@link ClassMapping}.
 * Based on {@link EntityJoinTree} for storing the joins structure and {@link EntityTreeInflater} for building the entities from
 * the {@link ResultSet}.
 * 
 * @author Guillaume Mary
 */
public class EntityMappingTreeSelectExecutor<C, I, T extends Table<T>> implements org.codefilarete.stalactite.engine.SelectExecutor<C, I> {
	
	protected final Logger LOGGER = LoggerFactory.getLogger(this.getClass());
	
	/** The delegate for joining the strategies, will help to build the SQL */
	private final EntityJoinTree<C, I> entityJoinTree;
	private final Dialect dialect;
	private final int blockSize;
	
	private final PrimaryKey<T, I> primaryKey;
	private final WhereClauseDMLNameProvider whereClauseDMLNameProvider;
	private final ConnectionProvider connectionProvider;
	private final IdentifierAssembler<I, T> identifierAssembler;
	
	protected SQLOperationListener<Column<T, ?>> operationListener;
	
	private final DMLGenerator dmlGenerator;
	private String rawQuery;
	private EntityTreeQuery<C> entityTreeQuery;
	private final ParameterBinderIndexFromMap<Column<T, ?>, ParameterBinder<?>> parameterBindersForPKInSelect;
	
	public EntityMappingTreeSelectExecutor(EntityMapping<C, I, T> entityMapping,
										   Dialect dialect,
										   ConnectionProvider connectionProvider) {
		this.dialect = dialect;
		this.identifierAssembler = entityMapping.getIdMapping().getIdentifierAssembler();
		this.entityJoinTree = new EntityJoinTree<>(new EntityMappingAdapter<>(entityMapping), entityMapping.getTargetTable());
		this.blockSize = dialect.getInOperatorMaxSize();
		this.primaryKey = entityMapping.getTargetTable().getPrimaryKey();
		// NB: in the condition, table and columns are from the main strategy, so there's no need to use aliases
		this.whereClauseDMLNameProvider = new WhereClauseDMLNameProvider(
				entityMapping.getTargetTable(),
				entityMapping.getTargetTable().getAbsoluteName(),
				dialect.getDmlNameProviderFactory());
		this.connectionProvider = connectionProvider;
		
		this.dmlGenerator = new DMLGenerator(dialect.getColumnBinderRegistry(), new NoopSorter(), k -> whereClauseDMLNameProvider);
		
		parameterBindersForPKInSelect = new ParameterBinderIndexFromMap<>(Iterables.map(primaryKey.getColumns(), Function.identity(), dialect.getColumnBinderRegistry()::getBinder));
		
		PersisterBuilderContext currentBuilderContext = PersisterBuilderContext.CURRENT.get();
		currentBuilderContext.addBuildLifeCycleListener(new BuildLifeCycleListener() {
			@Override
			public void afterBuild() {
				
			}
			
			@Override
			public void afterAllBuild() {
				prepareQuery();
			}
		});
	}
	
	@VisibleForTesting
	void prepareQuery() {
		ColumnBinderRegistry columnBinderRegistry = dialect.getColumnBinderRegistry();
		this.entityTreeQuery = new EntityTreeQueryBuilder<>(this.entityJoinTree, columnBinderRegistry).buildSelectQuery();
		QuerySQLBuilderFactory.QuerySQLBuilder sqlBuilder = dialect.getQuerySQLBuilderFactory().queryBuilder(entityTreeQuery.getQuery());
		this.rawQuery = sqlBuilder.toSQL();
	}
	
	public EntityJoinTree<C, I> getEntityJoinTree() {
		return entityJoinTree;
	}
	
	public void setOperationListener(SQLOperationListener<Column<T, ?>> operationListener) {
		this.operationListener = operationListener;
	}
	
	@Override
	public Set<C> select(Iterable<I> ids) {
		// cutting ids into pieces, adjusting expected result size
		List<List<I>> parcels = Collections.parcel(ids, blockSize);
		Set<C> result = new HashSet<>(parcels.size() * blockSize);
		LOGGER.debug("selecting entities in {} chunks", parcels.size());
		if (!parcels.isEmpty()) {
			// Creation of the where clause: we use a dynamic "in" operator clause to avoid multiple QueryBuilder instantiation
			DDLAppender identifierCriteria = new JoinDDLAppender(whereClauseDMLNameProvider);
			
			List<I> lastBlock = Iterables.last(parcels, java.util.Collections.emptyList());
			// keep only full blocks to run them on the fully filled "in" operator
			int lastBlockSize = lastBlock.size();
			if (lastBlockSize != blockSize) {
				parcels = Iterables.cutTail(parcels);
			}
			
			// Be aware that this executor is made to use same Connection to execute next SQL orders in same transaction
			InternalExecutor executor = newInternalExecutor(entityTreeQuery);
			if (!parcels.isEmpty()) {
				// change parameter mark count to adapt "in" operator values
				ParameterizedWhere<T> tableParameterizedWhere = dmlGenerator.appendTupledWhere(identifierCriteria, primaryKey.getColumns(), blockSize);
				result.addAll(executor.execute(rawQuery + " where " + identifierCriteria, parcels, tableParameterizedWhere.getColumnToIndex()));
			}
			if (!lastBlock.isEmpty()) {
				// change parameter mark count to adapt "in" operator values, we must clear previous where clause
				identifierCriteria.getAppender().setLength(0);
				ParameterizedWhere<T> tableParameterizedWhere = dmlGenerator.appendTupledWhere(identifierCriteria, primaryKey.getColumns(), lastBlock.size());
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
		private final Map<Selectable<?>, ParameterBinder<?>> selectParameterBinders;
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
				protected Set<C> transform(Iterator<? extends ColumnedRow> rowIterator, int resultSize) {
					return entityTreeInflater.transform(() -> (Iterator<ColumnedRow>) rowIterator, resultSize);
				}
			};
		}
		
		@VisibleForTesting
		List<C> execute(String sql, Collection<? extends List<I>> idsParcels, Map<Column<T, ?>, int[]> inOperatorValueIndexes) {
			// binders must be exactly the ones necessary to the request, else an IllegalArgumentException is thrown at execution time
			// so we have to extract them from what is in the request : only primary key columns are parameterized
			ColumnParameterizedSelect<T> preparedSelect = new ColumnParameterizedSelect<T>(
					sql,
					inOperatorValueIndexes,
					parameterBindersForPKInSelect.getParameterBinders(),
					selectParameterBinders,
					entityTreeQuery.getQuery().getAliases());
			List<C> result = new ArrayList<>(idsParcels.size() * blockSize);
			try (ReadOperation<Column<T, ?>> columnReadOperation = dialect.getReadOperationFactory().createInstance(preparedSelect, connectionProvider)) {
				columnReadOperation.setListener(EntityMappingTreeSelectExecutor.this.operationListener);
				for (List<I> parcel : idsParcels) {
					result.addAll(executor.execute(columnReadOperation, parcel));
				}
			}
			return result;
		}
	}
	
	/**
	 * Designed as both inheriting from {@link DMLNameProvider} for type compatibility with {@link org.codefilarete.stalactite.query.builder.SQLAppender}s
	 * methods, and as a wrapper of a {@link DMLNameProvider} to let user call a shared {@link DMLNameProviderFactory}.
	 * @author Guillaume Mary
	 */
	private static class WhereClauseDMLNameProvider extends DMLNameProvider {
		
		private final Table whereTable;
		private final String whereTableAlias;
		private final DMLNameProvider delegate;
		
		private WhereClauseDMLNameProvider(Table whereTable, @Nullable String whereTableAlias, DMLNameProviderFactory delegateFactory) {
			// we don't care about the aliases because we redefine our way of getting them, see #getAlias
			super(java.util.Collections.emptyMap());
			this.whereTable = whereTable;
			this.whereTableAlias = whereTableAlias;
			this.delegate = delegateFactory.build(new HashMap<>());
		}
		
		/** Overridden to get alias of the root and only for it, throws exception if given table is not where table one */
		@Override
		public String getAlias(Fromable table) {
			if (table == whereTable) {
				return Objects.preventNull(whereTableAlias, table.getName());
			} else {
				// anti unexpected usage
				throw new IllegalArgumentException("Table " + table.getName() + " is not expected to be used in the where clause");
			}
		}
		
		@Override
		public String getSimpleName(Selectable<?> column) {
			return delegate.getSimpleName(column);
		}
		
		@Override
		public String getTablePrefix(Fromable table) {
			return delegate.getTablePrefix(table);
		}
		
		@Override
		public String getName(Selectable<?> column) {
			return delegate.getName(column);
		}
	}
	
	/**
	 * A dedicated {@link DDLAppender} for joins : it prefixes columns with their table alias (or name)
	 */
	private static class JoinDDLAppender extends DDLAppender {
		
		private JoinDDLAppender(DMLNameProvider dmlNameProvider) {
			super(dmlNameProvider);
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
