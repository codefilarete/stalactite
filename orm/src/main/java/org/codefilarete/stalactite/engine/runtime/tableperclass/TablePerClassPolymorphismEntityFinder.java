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

import org.codefilarete.stalactite.engine.EntityReadExecutor;
import org.codefilarete.stalactite.engine.configurer.builder.BuildLifeCycleListener;
import org.codefilarete.stalactite.engine.configurer.builder.PersisterBuilderContext;
import org.codefilarete.stalactite.engine.configurer.resolver.polymorphism.tableperclass.TablePerClassAppender;
import org.codefilarete.stalactite.engine.configurer.resolver.polymorphism.tableperclass.TablePerClassAppender.TablePerClassUnion;
import org.codefilarete.stalactite.engine.runtime.AbstractPolymorphicEntityFinder;
import org.codefilarete.stalactite.engine.runtime.ConfiguredEntityReader;
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
import org.codefilarete.stalactite.mapping.EntityMapping;
import org.codefilarete.stalactite.mapping.id.assembly.IdentifierAssembler;
import org.codefilarete.stalactite.query.ConfiguredEntityCriteria;
import org.codefilarete.stalactite.query.api.QueryStatement.PseudoTable;
import org.codefilarete.stalactite.query.api.Selectable;
import org.codefilarete.stalactite.query.api.Selectable.SimpleSelectable;
import org.codefilarete.stalactite.query.builder.QuerySQLBuilderFactory.QuerySQLBuilder;
import org.codefilarete.stalactite.query.model.GroupBy;
import org.codefilarete.stalactite.query.model.Having;
import org.codefilarete.stalactite.query.model.Limit;
import org.codefilarete.stalactite.query.model.OrderBy;
import org.codefilarete.stalactite.query.model.Query;
import org.codefilarete.stalactite.query.model.Select;
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
 * 
 * 
 * @author Guillaume Mary
 */
public class TablePerClassPolymorphismEntityFinder<C, I, T extends Table<T>> extends AbstractPolymorphicEntityFinder<C, I, T> {
	
	@VisibleForTesting
	static final String DISCRIMINATOR_ALIAS = TablePerClassAppender.ENTITY_TYPE_DISCRIMINATOR_NAME;
	private static final SimpleSelectable<Integer> DISCRIMINATOR_COLUMN = new SimpleSelectable<>(DISCRIMINATOR_ALIAS, Integer.class);
	
	private final T mainTable;
	private final IdentifierAssembler<I, T> identifierAssembler;
	private final Set<? extends ConfiguredEntityReader<? extends C, I, ?>> subPersisters;
	private final SingleLoadEntityJoinTree<C, I> singleLoadEntityJoinTree;
	private final TablePerClassUnion<? extends C, I> tablePerClassUnion;
	private final EntityCriteriaSupport<C> criteriaSupport;
	private Query query;
	private EntityTreeQuery<C> entityTreeQuery;
	
	public <SUBENTITY extends C> TablePerClassPolymorphismEntityFinder(
			ConfiguredRelationalPersister<C, I, T> mainPersister,
			Set<? extends ConfiguredEntityReader<SUBENTITY, I, ?>> subPersisters,
			ConnectionProvider connectionProvider,
			Dialect dialect
	) {
		this(mainPersister.getEntityJoinTree(), mainPersister, subPersisters, connectionProvider, dialect);
	}
	
	public <SUBENTITY extends C> TablePerClassPolymorphismEntityFinder(
			EntityJoinTree<C, I> mainEntityJoinTree,
			ConfiguredEntityReader<C, I, T> mainReader,
			Set<? extends ConfiguredEntityReader<SUBENTITY, I, ?>> subPersisters,
			ConnectionProvider connectionProvider,
			Dialect dialect) {
		super(mainEntityJoinTree, mainReader, subPersisters, connectionProvider, dialect);
		
		this.identifierAssembler = mainReader.getMapping().getIdMapping().getIdentifierAssembler();
		this.mainTable = mainReader.getMapping().getTargetTable();
		// building readers and aliases for union-all query
		this.subPersisters = subPersisters;
		// we give all table columns to the union to, in particular, give the existing join columns which are needed by
		// the projectTo(..) method to build the final tree
		this.tablePerClassUnion = TablePerClassAppender.buildUnionForIdentifierRead(subPersisters, mainReader.<T>getMainTable().getColumns());
		this.singleLoadEntityJoinTree = buildSingleLoadEntityJoinTree(tablePerClassUnion);
		mainEntityJoinTree.projectTo(this.singleLoadEntityJoinTree, ROOT_JOIN_NAME);
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
	
	public TablePerClassPolymorphismEntityFinder(
			EntityJoinTree<C, I> mainEntityJoinTree,
			ConfiguredEntityReader<C, I, T> mainReader,
			Set<? extends ConfiguredEntityReader<? extends C, I, ?>> subPersisters,
			TablePerClassUnion<? extends C, I> tablePerClassUnion,
			ConnectionProvider connectionProvider,
			Dialect dialect) {
		super(mainEntityJoinTree, mainReader, subPersisters, connectionProvider, dialect);
		
		this.identifierAssembler = mainReader.getMapping().getIdMapping().getIdentifierAssembler();
		this.mainTable = mainReader.getMapping().getTargetTable();
		// building readers and aliases for union-all query
		this.subPersisters = subPersisters;
		this.singleLoadEntityJoinTree = buildSingleLoadEntityJoinTree(tablePerClassUnion);
		this.tablePerClassUnion = tablePerClassUnion;
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
	private SingleLoadEntityJoinTree<C, I> buildSingleLoadEntityJoinTree(TablePerClassUnion<? extends C, I> tablePerClassUnion) {
		// we add joins to the union clause
		SingleLoadEntityJoinTree<C, I> result = new SingleLoadEntityJoinTree<>(
				mainTable,
				mainReader.getMapping(),
				tablePerClassUnion.getSubtypeSelectorPerDiscriminatorValue(),
				tablePerClassUnion.asPseudoTable(mainTable.getName()),
				tablePerClassUnion.getDiscriminatorColumn());
		
		addTablePerClassPolymorphicSubPersistersJoins(result, tablePerClassUnion.getSubtypeSelectorPerDiscriminatorValue());
		return result;
	}
	
	private <SUBENTITY extends C, SUBTABLE extends Table<SUBTABLE>> void addTablePerClassPolymorphicSubPersistersJoins(
			SingleLoadEntityJoinTree<C, I> entityJoinTree,
			Map<Integer, ConfiguredEntityReader<SUBENTITY, I, ?>> discriminatorPerSubPersister) {
		
		discriminatorPerSubPersister.forEach((discriminatorValue, subPersister) -> {
			String mergeJoinName = entityJoinTree.<SUBENTITY, T, SUBTABLE, I>addMergeJoin(ROOT_JOIN_NAME,
					new EntityMergerAdapter<>(subPersister.getMapping()),
					mainTable.getPrimaryKey(),
					subPersister.<SUBTABLE>getMainTable().getPrimaryKey(),
					JoinType.OUTER,
					joinNode -> {
						MergeJoinRowConsumer<SUBENTITY> joinRowConsumer = new MergeJoinRowConsumer<>(
								(MergeJoinNode<SUBENTITY, ?, ?, ?>) joinNode,
                                subPersister.getMapping().getRowTransformer());
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
		Query queryClone = new Query(query.getSelect(), query.getFrom(), new Where(where.getCriteria()), new GroupBy(), new Having(), orderBy, limit);
		
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
		Query queryClone = new Query(new Select(), query.getFrom(), new Where(where.getCriteria()), new GroupBy(), new Having(), orderBy, limit);
		
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
		
		Map<ConfiguredEntityReader<? extends C, I, ?>, Set<I>> idsPerSubtype = readIds(sqlQueryBuilder.toPreparableSQL().toPreparedSQL(values), columnReaders, queryClone.getAliases());
		
		// Second phase : selecting entities by delegating it to each subclass loader
		// It will generate 1 query per found subclass, made as this :
		// - to avoid superfluous join and complex query in case of relation
		// - make it simpler to implement
		if (hasSubPolymorphicPersister) {
			LOGGER.debug("Asking sub-polymorphic persisters to load the entities");
			Set<C> result = new HashSet<>();
			idsPerSubtype.forEach((subreader, ids) -> result.addAll(subreader.select(ids)));
			return result;
		} else {
			Set<I> ids = idsPerSubtype.values().stream().flatMap(Collection::stream).collect(Collectors.toSet());
			return selectWithSingleQueryWhereIdIn(ids);
		}
	}
	
	private Map<ConfiguredEntityReader<? extends C, I, ?>, Set<I>> readIds(PreparedSQL preparedSQL, Map<Selectable<?>, ResultSetReader<?>> columnReaders, Map<Selectable<?>, String> aliases) {
		try (ReadOperation<Integer> closeableOperation = dialect.getReadOperationFactory().createInstance(preparedSQL, connectionProvider)) {
			ResultSet resultSet = closeableOperation.execute();
			ColumnedRowIterator rowIterator = new ColumnedRowIterator(resultSet, columnReaders, aliases);
			// Below we keep the order of given entities mainly to get steady unit tests. Meanwhile, this may have performance
			// impacts but it's very difficult to measure
			Map<ConfiguredEntityReader<? extends C, I, ?>, Set<I>> idsPerSubclass = new KeepOrderMap<>();
			rowIterator.forEachRemaining(row -> {
				// looking for entity type on row : we read each subclass PK and check for nullity. The non-null one is the good one
				Integer discriminatorValue = row.get(DISCRIMINATOR_COLUMN);
				
				// adding identifier to subclass' ids
				idsPerSubclass.computeIfAbsent(tablePerClassUnion.getSubtypeSelectorPerDiscriminatorValue().get(discriminatorValue), k -> new HashSet<>())
						.add(identifierAssembler.assemble(row));
			});
			return idsPerSubclass;
		} catch (RuntimeException e) {
			throw new SQLExecutionException(preparedSQL.getSQL(), e);
		}
	}
	
	static class SingleLoadEntityJoinTree<C, I> extends EntityJoinTree<C, I> {
		
		private final IdentityHashMap<Column<?, ?>, Selectable<?>> mainColumnToPseudoColumn = new IdentityHashMap<>();
		
		public <SUBENTITY extends C, T extends Table<T>> SingleLoadEntityJoinTree(T mainTable,
																				  EntityMapping<C, I, T> mainMapping,
																				  Map<Integer, ? extends EntityReadExecutor<SUBENTITY, I>> subPersisterPerDiscriminator,
																				  PseudoTable pseudoTable,
																				  SimpleSelectable<Integer> discriminatorColumn) {
			super(self -> new TablePerClassRootJoinNode<>(self, mainMapping, subPersisterPerDiscriminator, pseudoTable, discriminatorColumn));
			// Building a mapping between main persister columns and those of union
			// this will allow us to lookup for main persister columns values in final ResultSet
			pseudoTable.getColumns().forEach(pseudoColumn -> {
				Column column = mainTable.findColumn(pseudoColumn.getExpression());
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
