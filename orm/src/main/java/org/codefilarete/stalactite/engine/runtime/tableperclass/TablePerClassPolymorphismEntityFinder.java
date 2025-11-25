package org.codefilarete.stalactite.engine.runtime.tableperclass;

import java.sql.ResultSet;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.codefilarete.stalactite.engine.configurer.builder.PersisterBuilderContext;
import org.codefilarete.stalactite.engine.configurer.builder.BuildLifeCycleListener;
import org.codefilarete.stalactite.engine.runtime.AbstractPolymorphicEntityFinder;
import org.codefilarete.stalactite.engine.runtime.ConfiguredRelationalPersister;
import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree;
import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree.JoinType;
import org.codefilarete.stalactite.engine.runtime.load.EntityMerger.EntityMergerAdapter;
import org.codefilarete.stalactite.engine.runtime.load.EntityTreeInflater;
import org.codefilarete.stalactite.engine.runtime.load.EntityTreeQueryBuilder;
import org.codefilarete.stalactite.engine.runtime.load.EntityTreeQueryBuilder.EntityTreeQuery;
import org.codefilarete.stalactite.engine.runtime.load.MergeJoinNode;
import org.codefilarete.stalactite.engine.runtime.load.MergeJoinNode.MergeJoinRowConsumer;
import org.codefilarete.stalactite.engine.runtime.load.TablePerClassRootJoinNode;
import org.codefilarete.stalactite.engine.runtime.query.EntityCriteriaSupport;
import org.codefilarete.stalactite.engine.runtime.query.EntityQueryCriteriaSupport;
import org.codefilarete.stalactite.mapping.id.assembly.IdentifierAssembler;
import org.codefilarete.stalactite.query.ConfiguredEntityCriteria;
import org.codefilarete.stalactite.query.builder.QuerySQLBuilderFactory.QuerySQLBuilder;
import org.codefilarete.stalactite.query.model.GroupBy;
import org.codefilarete.stalactite.query.model.Having;
import org.codefilarete.stalactite.query.model.Limit;
import org.codefilarete.stalactite.query.model.Operators;
import org.codefilarete.stalactite.query.model.OrderBy;
import org.codefilarete.stalactite.query.model.Query;
import org.codefilarete.stalactite.query.model.QueryEase;
import org.codefilarete.stalactite.query.model.QueryStatement.PseudoTable;
import org.codefilarete.stalactite.query.model.Select;
import org.codefilarete.stalactite.query.model.Selectable;
import org.codefilarete.stalactite.query.model.Selectable.SimpleSelectable;
import org.codefilarete.stalactite.query.model.Union;
import org.codefilarete.stalactite.query.model.Where;
import org.codefilarete.stalactite.sql.ConnectionProvider;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.ColumnedRow;
import org.codefilarete.stalactite.sql.result.ColumnedRowIterator;
import org.codefilarete.stalactite.sql.statement.PreparedSQL;
import org.codefilarete.stalactite.sql.statement.ReadOperation;
import org.codefilarete.stalactite.sql.statement.SQLExecutionException;
import org.codefilarete.stalactite.sql.statement.binder.ResultSetReader;
import org.codefilarete.tool.VisibleForTesting;
import org.codefilarete.tool.collection.KeepOrderMap;

import static org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree.ROOT_JOIN_NAME;

/**
 * @author Guillaume Mary
 */
public class TablePerClassPolymorphismEntityFinder<C, I, T extends Table<T>> extends AbstractPolymorphicEntityFinder<C, I, T> {
	
	@VisibleForTesting
	static final String DISCRIMINATOR_ALIAS = "DISCRIMINATOR";
	private static final SimpleSelectable<String> DISCRIMINATOR_COLUMN = new SimpleSelectable<>(DISCRIMINATOR_ALIAS, String.class);
	
	private final ConfiguredRelationalPersister<C, I> mainPersister;
	private final IdentifierAssembler<I, T> identifierAssembler;
	private final T mainTable;
	private final Map<String, Class> discriminatorValues;
	private final SingleLoadEntityJoinTree<C, I> singleLoadEntityJoinTree;
	private final EntityCriteriaSupport<C> criteriaSupport;
	private Query query;
	private EntityTreeQuery<C> entityTreeQuery;
	
	public TablePerClassPolymorphismEntityFinder(
			ConfiguredRelationalPersister<C, I> mainPersister,
			Map<? extends Class<C>, ? extends ConfiguredRelationalPersister<C, I>> persisterPerSubclass,
			ConnectionProvider connectionProvider,
			Dialect dialect
	) {
		super(mainPersister, persisterPerSubclass, connectionProvider, dialect);
		this.mainPersister = mainPersister;
		this.identifierAssembler = mainPersister.getMapping().getIdMapping().getIdentifierAssembler();
		this.mainTable = mainPersister.getMainTable();
		// building readers and aliases for union-all query
		this.discriminatorValues = new HashMap<>();
		persisterPerSubclass.forEach((subEntityType, subEntityTable) -> {
			discriminatorValues.put(subEntityType.getSimpleName(), subEntityType);
		});
		this.singleLoadEntityJoinTree = buildSingleLoadEntityJoinTree();
		this.criteriaSupport = new EntityCriteriaSupport<>(singleLoadEntityJoinTree);
		
		// made for optimization (to avoid creating multiple times the query) but also to avoid adding several times the polymorphic JoinNode consumers
		// to itself by calling several time the "toConsumer(..)" method that usually calls some "add" method, which declares duplicates of consumers,
		// which, at the very end, implies an exception during the aggregate inflation phase : "Can't find consumer node"
		PersisterBuilderContext.CURRENT.get().addBuildLifeCycleListener(new BuildLifeCycleListener() {
			@Override
			public void afterBuild() {
			}
			
			@Override
			public void afterAllBuild() {
				buildQuery();
			}
		});
	}
	
	private void buildQuery() {
		entityTreeQuery = new EntityTreeQueryBuilder<>(singleLoadEntityJoinTree, dialect.getColumnBinderRegistry()).buildSelectQuery();
		query = entityTreeQuery.getQuery();
	}
	
	@Override
	protected EntityTreeQuery<C> getAggregateQueryTemplate() {
		return entityTreeQuery;
	}
	
	@Override
	public EntityJoinTree<C, I> getEntityJoinTree() {
		return singleLoadEntityJoinTree;
	}
	
	@Override
	public EntityQueryCriteriaSupport<C, I> newCriteriaSupport() {
		return new EntityQueryCriteriaSupport<>(this, criteriaSupport.copy());
	}
	
	/**
	 * Creates an {@link EntityJoinTree} which main Table is actually a Union clause made of sub-entities tables
	 * @return an appropriate {@link EntityJoinTree}
	 */
	private SingleLoadEntityJoinTree<C, I> buildSingleLoadEntityJoinTree() {
		Union union = new Union();
		Set<Column<T, ?>> allColumnsInHierarchy = mainPersister.<T>getMainTable().getColumns();
		
		Map<String, ConfiguredRelationalPersister<C, I>> discriminatorPerSubPersister = new HashMap<>();
		persisterPerSubclass.forEach((subEntityType, subEntityPersister) -> {
			String discriminatorValue = subEntityType.getSimpleName();
			
			subEntityPersister.getMainTable().getColumns();
			Map<Selectable<?>, String> subQueryColumns = new KeepOrderMap<>();
			allColumnsInHierarchy.forEach(pseudoColumn -> {
				Column column = subEntityPersister.getMainTable().findColumn(pseudoColumn.getExpression());
				if (column != null) {
					subQueryColumns.put(column, column.getName());
				} else {
					subQueryColumns.put(Operators.cast((String) null, pseudoColumn.getJavaType()), pseudoColumn.getExpression());
				}
			});
			subQueryColumns.put(new SimpleSelectable<>("'" + discriminatorValue + "'", String.class), DISCRIMINATOR_ALIAS);
			Query subQuery = QueryEase.
					select(subQueryColumns)
					.from(subEntityPersister.getMainTable())
					.getQuery();
			union.getQueries().add(subQuery);
			discriminatorPerSubPersister.put(discriminatorValue, subEntityPersister);
		});
		allColumnsInHierarchy.forEach(column -> {
			union.registerColumn(column.getExpression(), column.getJavaType(), column.getName());
		});
		union.registerColumn(DISCRIMINATOR_COLUMN.getExpression(), String.class, DISCRIMINATOR_ALIAS);
		
		// Note that it's very important to use main table name to mimic virtual main table else joins (below) won't work
		PseudoTable pseudoTable = union.asPseudoTable(mainTable.getName());
		// we add joins to the union clause
		SingleLoadEntityJoinTree<C, I> result = new SingleLoadEntityJoinTree<>(mainPersister, discriminatorPerSubPersister, pseudoTable, DISCRIMINATOR_COLUMN);
		mainEntityJoinTree.projectTo(result, ROOT_JOIN_NAME);
		
		addTablePerClassPolymorphicSubPersistersJoins(result, discriminatorPerSubPersister);
		return result;
	}
	
	private <V extends C, T1 extends Table<T1>, T2 extends Table<T2>> void addTablePerClassPolymorphicSubPersistersJoins(
			SingleLoadEntityJoinTree<C, I> entityJoinTree,
			Map<String, ConfiguredRelationalPersister<C, I>> discriminatorPerSubPersister) {
		
		discriminatorPerSubPersister.forEach((discriminatorValue, subPersister) -> {
						ConfiguredRelationalPersister<V, I> localSubPersister = (ConfiguredRelationalPersister<V, I>) subPersister;
			String mergeJoinName = entityJoinTree.<V, T1, T2, I>addMergeJoin(EntityJoinTree.ROOT_JOIN_NAME,
					new EntityMergerAdapter<>(localSubPersister.<T2>getMapping()),
					mainPersister.<T1>getMainTable().getPrimaryKey(),
					subPersister.<T2>getMainTable().getPrimaryKey(),
					JoinType.OUTER,
					joinNode -> {
						MergeJoinRowConsumer<V> joinRowConsumer = new MergeJoinRowConsumer<>(
								(MergeJoinNode<V, ?, ?, ?>) joinNode,
                                localSubPersister.<T2>getMapping().getRowTransformer());
						entityJoinTree.getRoot().addSubPersister(subPersister, joinRowConsumer, discriminatorValue);
						return joinRowConsumer;
					}
			);
			// we add the joins of the sub-persister to the whole graph to make it load its relations
			subPersister.getEntityJoinTree().projectTo(entityJoinTree, mergeJoinName);
		});
	}
	
	@Override
	public Set<C> selectWithSingleQuery(ConfiguredEntityCriteria where, Map<String, Object> values, OrderBy orderBy, Limit limit) {
		LOGGER.debug("Finding entities in a single query with criteria {}", where);
		if (hasSubPolymorphicPersister) {
			LOGGER.debug("Single query was asked but due to sub-polymorphism the query is made in 2 phases");
			return selectIn2Phases(where, values, orderBy, limit);
		} else {
			return localSelectWithSingleQuery(where, orderBy, limit);
		}
	}
	
	private Set<C> localSelectWithSingleQuery(ConfiguredEntityCriteria where, OrderBy orderBy, Limit limit) {
		// we clone the query to avoid polluting the instance one, else, from select(..) to select(..), we append the criteria at the end of it,
		// which makes the query usually returning no data (because of the condition mix)
		Query queryClone = new Query(query.getSelectDelegate(), query.getFromDelegate(), new Where<>(where.getCriteria()), new GroupBy(), new Having(), orderBy, limit);
		
		// since criteria is passed to union subqueries, we don't need it into the entire query
		QuerySQLBuilder sqlQueryBuilder = dialect.getQuerySQLBuilderFactory().queryBuilder(queryClone);
		EntityTreeInflater<C> inflater = entityTreeQuery.getInflater();
		PreparedSQL preparedSQL = sqlQueryBuilder.toPreparableSQL().toPreparedSQL(new HashMap<>());
		try (ReadOperation<Integer> readOperation = dialect.getReadOperationFactory().createInstance(preparedSQL, connectionProvider)) {
			ResultSet resultSet = readOperation.execute();
			// NB: we give the same ParametersBinders of those given at ColumnParameterizedSelect since the row iterator is expected to read column from it
			Iterator<? extends ColumnedRow> rowIterator = new ColumnedRowIterator(resultSet, entityTreeQuery.getSelectParameterBinders(), entityTreeQuery.getColumnAliases());
			return inflater.transform(() -> (Iterator<ColumnedRow>) rowIterator, 50);
		} catch (RuntimeException e) {
			throw new SQLExecutionException(preparedSQL.getSQL(), e);
		}
	}
	
	@Override
	public Set<C> selectIn2Phases(ConfiguredEntityCriteria where, Map<String, Object> values, OrderBy orderBy, Limit limit) {
		LOGGER.debug("Finding entities in 2-phases query with criteria {}", where);
		// we clone the query to avoid polluting the instance one, else, from select(..) to select(..), we append the criteria at the end of it,
		// which makes the query usually returning no data (because of the condition mix).
		// Note that we don't need to clone the select clause because we register the columns that we need some lines below
		Query queryClone = new Query(new Select(), query.getFromDelegate(), new Where<>(where.getCriteria()), new GroupBy(), new Having(), orderBy, limit);
		
		// since criteria is passed to union subqueries, we don't need it into the entire query
		QuerySQLBuilder sqlQueryBuilder = dialect.getQuerySQLBuilderFactory().queryBuilder(queryClone);
		
		// First phase : selecting ids (made by clearing selected elements for performance issue)
		mainTable.getPrimaryKey().getColumns().forEach(pkColumn -> {
			Selectable<?> column = mainEntityJoinTree.getRoot().getTable().findColumn(pkColumn.getName());
			queryClone.select(column, pkColumn.getName());
		});
		queryClone.select(DISCRIMINATOR_COLUMN, DISCRIMINATOR_ALIAS);
		
		// selecting ids and their entity type
		Map<Selectable<?>, ResultSetReader<?>> columnReaders = new HashMap<>();
		queryClone.getColumns().forEach((selectable) -> {
			ResultSetReader<?> reader;
			if (selectable instanceof Column) {
				reader = dialect.getColumnBinderRegistry().getReader((Column) selectable);
			} else {
				reader = dialect.getColumnBinderRegistry().getReader(selectable.getJavaType());
			}
			columnReaders.put(selectable, reader);
		});
		columnReaders.put(DISCRIMINATOR_COLUMN, dialect.getColumnBinderRegistry().getBinder(DISCRIMINATOR_COLUMN.getJavaType()));
		
		Map<Class, Set<I>> idsPerSubtype = readIds(sqlQueryBuilder.toPreparableSQL().toPreparedSQL(values), columnReaders, queryClone.getAliases());
		
		// Second phase : selecting entities by delegating it to each subclass loader
		// It will generate 1 query per found subclass, made as this :
		// - to avoid superfluous join and complex query in case of relation
		// - make it simpler to implement
		Set<I> ids = idsPerSubtype.values().stream().flatMap(Collection::stream).collect(Collectors.toSet());
		
		if (hasSubPolymorphicPersister) {
			LOGGER.debug("Asking sub-polymorphic persisters to load the entities");
			Set<C> result = new HashSet<>();
			idsPerSubtype.forEach((k, v) -> result.addAll(persisterPerSubclass.get(k).select(v)));
			return result;
		} else {
			return selectWithSingleQueryWhereIdIn(ids);
		}
	}
	
	private Map<Class, Set<I>> readIds(PreparedSQL preparedSQL, Map<Selectable<?>, ResultSetReader<?>> columnReaders, Map<Selectable<?>, String> aliases) {
		try (ReadOperation<Integer> closeableOperation = dialect.getReadOperationFactory().createInstance(preparedSQL, connectionProvider)) {
			ResultSet resultSet = closeableOperation.execute();
			ColumnedRowIterator rowIterator = new ColumnedRowIterator(resultSet, columnReaders, aliases);
			// Below we keep the order of given entities mainly to get steady unit tests. Meanwhile, this may have performance
			// impacts but it's very difficult to measure
			Map<Class, Set<I>> idsPerSubclass = new KeepOrderMap<>();
			rowIterator.forEachRemaining(row -> {
				// looking for entity type on row : we read each subclass PK and check for nullity. The non-null one is the good one
				String discriminatorValue = row.get(DISCRIMINATOR_COLUMN);
				// NB: we trim because some database (as HSQLDB) adds some padding in order that all values get same length
				Class<? extends C> entitySubclass = discriminatorValues.get(discriminatorValue.trim());
				
				// adding identifier to subclass' ids
				idsPerSubclass.computeIfAbsent(entitySubclass, k -> new HashSet<>())
						.add(identifierAssembler.assemble(row));
			});
			return idsPerSubclass;
		} catch (RuntimeException e) {
			throw new SQLExecutionException(preparedSQL.getSQL(), e);
		}
	}
	
	static class SingleLoadEntityJoinTree<C, I> extends EntityJoinTree<C, I> {
		
		private final IdentityHashMap<Column<?, ?>, Selectable<?>> mainColumnToPseudoColumn = new IdentityHashMap<>();
		
		public <T extends Table<T>> SingleLoadEntityJoinTree(ConfiguredRelationalPersister<C, I> mainPersister,
															 Map<String, ConfiguredRelationalPersister<C, I>> subPersisterPerDiscriminator,
															 PseudoTable pseudoTable,
															 SimpleSelectable<String> discriminatorColumn) {
			super(self -> new TablePerClassRootJoinNode<>(self, mainPersister, subPersisterPerDiscriminator, pseudoTable, discriminatorColumn));
			// Building a mapping between main persister columns and those of union
			// this will allow us to lookup for main persister columns values in final ResultSet
			pseudoTable.getColumns().forEach(pseudoColumn -> {
				Column column = mainPersister.getMainTable().findColumn(pseudoColumn.getExpression());
				if (column != null) {
					mainColumnToPseudoColumn.put(column, pseudoColumn);
				}
			});
		}
		
		@Override
		public TablePerClassRootJoinNode<C, I> getRoot() {
			return (TablePerClassRootJoinNode<C, I>) super.getRoot();
		}
		
		public IdentityHashMap<Column<?, ?>, Selectable<?>> getMainColumnToPseudoColumn() {
			return mainColumnToPseudoColumn;
		}
	}
}
