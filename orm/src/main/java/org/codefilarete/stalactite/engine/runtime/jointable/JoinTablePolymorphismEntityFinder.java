package org.codefilarete.stalactite.engine.runtime.jointable;

import java.sql.ResultSet;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

import org.codefilarete.stalactite.engine.configurer.PersisterBuilderContext;
import org.codefilarete.stalactite.engine.configurer.PersisterBuilderImpl.BuildLifeCycleListener;
import org.codefilarete.stalactite.engine.runtime.AbstractPolymorphicEntityFinder;
import org.codefilarete.stalactite.engine.runtime.ConfiguredRelationalPersister;
import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree;
import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree.JoinType;
import org.codefilarete.stalactite.engine.runtime.load.EntityMerger.EntityMergerAdapter;
import org.codefilarete.stalactite.engine.runtime.load.EntityTreeQueryBuilder;
import org.codefilarete.stalactite.engine.runtime.load.EntityTreeQueryBuilder.EntityTreeQuery;
import org.codefilarete.stalactite.engine.runtime.load.JoinTableRootJoinNode;
import org.codefilarete.stalactite.engine.runtime.load.MergeJoinNode;
import org.codefilarete.stalactite.engine.runtime.load.MergeJoinNode.MergeJoinRowConsumer;
import org.codefilarete.stalactite.engine.runtime.query.EntityCriteriaSupport;
import org.codefilarete.stalactite.engine.runtime.query.EntityQueryCriteriaSupport;
import org.codefilarete.stalactite.query.ConfiguredEntityCriteria;
import org.codefilarete.stalactite.query.builder.QuerySQLBuilderFactory.QuerySQLBuilder;
import org.codefilarete.stalactite.query.model.GroupBy;
import org.codefilarete.stalactite.query.model.Having;
import org.codefilarete.stalactite.query.model.Limit;
import org.codefilarete.stalactite.query.model.OrderBy;
import org.codefilarete.stalactite.query.model.Query;
import org.codefilarete.stalactite.query.model.Selectable;
import org.codefilarete.stalactite.query.model.Where;
import org.codefilarete.stalactite.sql.ConnectionProvider;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.ColumnedRowIterator;
import org.codefilarete.stalactite.sql.statement.PreparedSQL;
import org.codefilarete.stalactite.sql.statement.ReadOperation;
import org.codefilarete.stalactite.sql.statement.SQLExecutionException;
import org.codefilarete.stalactite.sql.statement.binder.ResultSetReader;
import org.codefilarete.tool.Duo;
import org.codefilarete.tool.collection.KeepOrderMap;

import static org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree.ROOT_JOIN_NAME;

/**
 * @author Guillaume Mary
 */
public class JoinTablePolymorphismEntityFinder<C, I, T extends Table<T>> extends AbstractPolymorphicEntityFinder<C, I, T> {
	
	private final SingleLoadEntityJoinTree<C, I> singleLoadEntityJoinTree;
	private final EntityCriteriaSupport<C> criteriaSupport;
	
	private Query query;
	private EntityTreeQuery<C> entityTreeQuery;
	
	public JoinTablePolymorphismEntityFinder(
			ConfiguredRelationalPersister<C, I> mainPersister,
			Map<? extends Class<C>, ? extends ConfiguredRelationalPersister<C, I>> persisterPerSubclass,
			ConnectionProvider connectionProvider,
			Dialect dialect) {
		super(mainPersister, persisterPerSubclass, connectionProvider, dialect);
		this.singleLoadEntityJoinTree = buildSingleLoadEntityJoinTree(mainPersister);
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
	
	private SingleLoadEntityJoinTree<C, I> buildSingleLoadEntityJoinTree(ConfiguredRelationalPersister<C, I> mainPersister) {
		SingleLoadEntityJoinTree<C, I> result = new SingleLoadEntityJoinTree<>(
				mainPersister,
				new HashSet<>(persisterPerSubclass.values())
		);
		// sub entities persisters will be used to create entities
		persisterPerSubclass.forEach((type, persister) -> {
			String mergeJoin = result.addMergeJoin(
                    ROOT_JOIN_NAME,
					new EntityMergerAdapter<>(persister.<T>getMapping()),
					mainPersister.getMainTable().getPrimaryKey(),
					persister.getMainTable().getPrimaryKey(),
					JoinType.OUTER,
					joinNode -> {
						// implemented to add newly created sub consumer to root one, therefore it will be able to create the right sub-instance
						MergeJoinRowConsumer<C> subEntityConsumer = new MergeJoinRowConsumer<>((MergeJoinNode<C, ?, ?, ?>) joinNode, persister.getMapping().getRowTransformer());
						result.getRoot().addSubPersister(persister, subEntityConsumer);
						return subEntityConsumer;
					}
			);
			// we add all relations of sub-persister to the join tree to make the data available in the final query
			// because JoinTableRootJoinNode of SingleLoadEntityJoinTree need them
			persister.getEntityJoinTree().projectTo(result, mergeJoin);
		});
		// we project main persister tree to keep its relations
		mainEntityJoinTree.projectTo(result, ROOT_JOIN_NAME);
		return result;
	}
	
	@Override
	public Set<C> selectWithSingleQuery(ConfiguredEntityCriteria where, Map<String, Object> values, OrderBy orderBy, Limit limit) {
		LOGGER.debug("Finding entities in a single query with criteria {}", where);
		if (hasSubPolymorphicPersister) {
			LOGGER.debug("Single query was asked but due to sub-polymorphism the query is made in 2 phases");
			return selectIn2Phases(where, values, orderBy, limit);
		} else {
			Query queryClone = new Query(query.getSelectDelegate(), query.getFromDelegate(), new Where<>().add(where.getCriteria()), new GroupBy(), new Having(), orderBy, limit);
			return super.selectWithSingleQuery(queryClone, values, entityTreeQuery, dialect, connectionProvider);
		}
	}
	
	@Override
	public Set<C> selectIn2Phases(ConfiguredEntityCriteria where, Map<String, Object> values, OrderBy orderBy, Limit limit) {
		LOGGER.debug("Finding entities in 2-phases query with criteria {}", where);
		
		// we clone the query to avoid polluting the instance one, else, from select(..) to select(..), we append the criteria at the end of it,
		// which makes the query usually returning no data (because of the condition mix)
		Query queryClone = new Query(query.getSelectDelegate(), query.getFromDelegate(), new Where<>(where.getCriteria()), new GroupBy(), new Having(), orderBy, limit);
		
		QuerySQLBuilder sqlQueryBuilder = dialect.getQuerySQLBuilderFactory().queryBuilder(queryClone);
		
		// selecting ids and their entity type
		Map<Selectable<?>, ResultSetReader<?>> columnReaders = new HashMap<>();
		queryClone.getColumns().forEach((selectable) -> columnReaders.put(selectable, dialect.getColumnBinderRegistry().getBinder((Column) selectable)));
		
		Map<Class, Set<I>> idsPerSubtype = readIds(sqlQueryBuilder.toPreparableSQL().toPreparedSQL(values), columnReaders, queryClone.getAliases());
		
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
		// Below we keep the order of given entities mainly to get steady unit tests. Meanwhile, this may have performance
		// impacts but it's very difficult to measure
		Map<Class, Set<I>> result = new KeepOrderMap<>();
		try (ReadOperation<Integer> readOperation = dialect.getReadOperationFactory().createInstance(preparedSQL, connectionProvider)) {
			ResultSet resultSet = readOperation.execute();
			
			ColumnedRowIterator resultSetIterator = new ColumnedRowIterator(resultSet, columnReaders, aliases);
			resultSetIterator.forEachRemaining(row -> {
				
				// looking for entity type on row : we read each subclass PK and check for nullity. The non-null one is the 
				// right one
				Set<Entry<Class<C>, ConfiguredRelationalPersister<C, I>>> entries = persisterPerSubclass.entrySet();
				Duo<Class, I> duo = null;
				I identifier;
				for (Entry<Class<C>, ConfiguredRelationalPersister<C, I>> entry : entries) {
					identifier = entry.getValue().getMapping().getIdMapping().getIdentifierAssembler().assemble(row);
					if (identifier != null) {
						duo = new Duo<>(entry.getKey(), identifier);
						break;
					}
				}
				result.computeIfAbsent(duo.getLeft(), k -> new HashSet<>()).add(duo.getRight());
			});
			return result;
		} catch (RuntimeException e) {
			throw new SQLExecutionException(preparedSQL.getSQL(), e);
		}
	}
	
	/**
	 * Appropriate {@link EntityJoinTree} to instantiate {@link JoinTableRootJoinNode} as root in order to handle join-node polymorphism of root entity. 
	 * @param <C>
	 * @param <I>
	 * @author Guillaume Mary
	 */
	private static class SingleLoadEntityJoinTree<C, I> extends EntityJoinTree<C, I> {
		
		public <T extends Table<T>> SingleLoadEntityJoinTree(ConfiguredRelationalPersister<C, I> mainPersister,
															 Set<? extends ConfiguredRelationalPersister<C, I>> subPersisters) {
			super(tree -> new JoinTableRootJoinNode<>(
					tree,
					mainPersister,
					subPersisters,
					mainPersister.<T>getMapping().getSelectableColumns(),
					(T) mainPersister.getMainTable())
			);
		}
		
		@Override
		public JoinTableRootJoinNode<C, I, ?> getRoot() {
			return (JoinTableRootJoinNode<C, I, ?>) super.getRoot();
		}
	}
}
