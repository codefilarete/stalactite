package org.codefilarete.stalactite.engine.runtime.singletable;

import java.sql.ResultSet;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.codefilarete.stalactite.dsl.PolymorphismPolicy.SingleTablePolymorphism;
import org.codefilarete.stalactite.engine.configurer.PersisterBuilderContext;
import org.codefilarete.stalactite.engine.configurer.PersisterBuilderImpl.BuildLifeCycleListener;
import org.codefilarete.stalactite.engine.runtime.AbstractPolymorphicEntityFinder;
import org.codefilarete.stalactite.engine.runtime.ConfiguredRelationalPersister;
import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree;
import org.codefilarete.stalactite.engine.runtime.load.EntityTreeQueryBuilder;
import org.codefilarete.stalactite.engine.runtime.load.EntityTreeQueryBuilder.EntityTreeQuery;
import org.codefilarete.stalactite.engine.runtime.load.JoinTableRootJoinNode;
import org.codefilarete.stalactite.engine.runtime.load.SingleTableRootJoinNode;
import org.codefilarete.stalactite.engine.runtime.query.EntityCriteriaSupport;
import org.codefilarete.stalactite.engine.runtime.query.EntityQueryCriteriaSupport;
import org.codefilarete.stalactite.mapping.id.assembly.IdentifierAssembler;
import org.codefilarete.stalactite.query.ConfiguredEntityCriteria;
import org.codefilarete.stalactite.query.builder.QuerySQLBuilderFactory.QuerySQLBuilder;
import org.codefilarete.stalactite.query.model.GroupBy;
import org.codefilarete.stalactite.query.model.Having;
import org.codefilarete.stalactite.query.model.Limit;
import org.codefilarete.stalactite.query.model.OrderBy;
import org.codefilarete.stalactite.query.model.Query;
import org.codefilarete.stalactite.query.model.Select;
import org.codefilarete.stalactite.query.model.Selectable;
import org.codefilarete.stalactite.query.model.Where;
import org.codefilarete.stalactite.sql.ConnectionProvider;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.PrimaryKey;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.ColumnedRowIterator;
import org.codefilarete.stalactite.sql.statement.PreparedSQL;
import org.codefilarete.stalactite.sql.statement.ReadOperation;
import org.codefilarete.stalactite.sql.statement.SQLExecutionException;
import org.codefilarete.stalactite.sql.statement.binder.ResultSetReader;
import org.codefilarete.tool.VisibleForTesting;
import org.codefilarete.tool.collection.KeepOrderMap;
import org.codefilarete.tool.collection.KeepOrderSet;

import static org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree.ROOT_JOIN_NAME;

/**
 * @author Guillaume Mary
 */
public class SingleTablePolymorphismEntityFinder<C, I, T extends Table<T>, DTYPE> extends AbstractPolymorphicEntityFinder<C, I, T> {
	
	@VisibleForTesting
	static final String DISCRIMINATOR_ALIAS = "DISCRIMINATOR";
	
	private final IdentifierAssembler<I, T> identifierAssembler;
	private final Column<T, DTYPE> discriminatorColumn;
	private final SingleTablePolymorphism<C, DTYPE> polymorphismPolicy;
	private final EntityJoinTree<C, I> singleLoadEntityJoinTree;
	private final EntityCriteriaSupport<C> criteriaSupport;
	private Query query;
	private EntityTreeQuery<C> entityTreeQuery;
	
	public SingleTablePolymorphismEntityFinder(ConfiguredRelationalPersister<C, I> mainPersister,
											   Map<? extends Class<C>, ? extends ConfiguredRelationalPersister<C, I>> persisterPerSubclass,
											   Column<T, DTYPE> discriminatorColumn,
											   SingleTablePolymorphism<C, DTYPE> polymorphismPolicy,
											   ConnectionProvider connectionProvider,
											   Dialect dialect) {
		super(mainPersister, persisterPerSubclass, connectionProvider, dialect);
		this.identifierAssembler = mainPersister.getMapping().getIdMapping().getIdentifierAssembler();
		this.discriminatorColumn = discriminatorColumn;
		this.polymorphismPolicy = polymorphismPolicy;
		this.singleLoadEntityJoinTree = buildSingleLoadEntityJoinTree(mainPersister, persisterPerSubclass);
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
	
	private SingleLoadEntityJoinTree<C, I, DTYPE> buildSingleLoadEntityJoinTree(ConfiguredRelationalPersister<C, I> mainPersister, Map<? extends Class<C>, ? extends ConfiguredRelationalPersister<C, I>> persisterPerSubclass) {
		SingleLoadEntityJoinTree<C, I, DTYPE> result = new SingleLoadEntityJoinTree<>(
				mainPersister,
				new HashSet<>(persisterPerSubclass.values()),
				discriminatorColumn,
				polymorphismPolicy
		);
		// we project main persister tree to keep its relations
		mainPersister.getEntityJoinTree().projectTo(result, ROOT_JOIN_NAME);
		// we add the joins of the sub-persister to the whole graph to make it load its relations
		persisterPerSubclass.values().forEach(subPersister -> {
			subPersister.getEntityJoinTree().projectTo(result, ROOT_JOIN_NAME);
		});
		return result;
	}
	
	@Override
	public Set<C> selectWithSingleQuery(ConfiguredEntityCriteria where, Map<String, Object> values, OrderBy orderBy, Limit limit) {
		LOGGER.debug("Finding entities in a single query with criteria {}", where);
		if (hasSubPolymorphicPersister) {
			LOGGER.debug("Single query was asked but due to sub-polymorphism the query is made in 2 phases");
			return selectIn2Phases(where, values, orderBy, limit);
		} else {
			Where add = new Where<>();
			add.add(query.getWhereDelegate().getConditions());
			add.add(where.getCriteria());
			Query queryClone = new Query(query.getSelectDelegate(), query.getFromDelegate(), add, new GroupBy(), new Having(), orderBy, limit);
			return super.selectWithSingleQuery(queryClone, values, entityTreeQuery, dialect, connectionProvider);
		}
	}
	
	@Override
	public Set<C> selectIn2Phases(ConfiguredEntityCriteria where, Map<String, Object> values, OrderBy orderBy, Limit limit) {
		LOGGER.debug("Finding entities in 2-phases query with criteria {}", where);
		
		// we clone the query to avoid polluting the instance one, else, from select(..) to select(..), we append the criteria at the end of it,
		// which makes the query usually returning no data (because of the condition mix)
		Query queryClone = new Query(new Select(), query.getFromDelegate(), new Where<>(where.getCriteria()), new GroupBy(), new Having(), orderBy, limit);
		
		QuerySQLBuilder sqlQueryBuilder = dialect.getQuerySQLBuilderFactory().queryBuilder(queryClone);
		
		// First phase : selecting ids (made by clearing selected elements for performance issue)
		PrimaryKey<T, I> pk = ((T) mainEntityJoinTree.getRoot().getTable()).getPrimaryKey();
		pk.getColumns().forEach(column -> queryClone.select(column, column.getAlias()));
		queryClone.select(discriminatorColumn, DISCRIMINATOR_ALIAS);
		
		// selecting ids and their entity type
		Map<Selectable<?>, ResultSetReader<?>> columnReaders = new HashMap<>();
		queryClone.getColumns().forEach(selectable -> columnReaders.put(selectable, dialect.getColumnBinderRegistry().getBinder((Column) selectable)));
		
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
			Map<Class, Set<I>> result = new KeepOrderMap<>();
			rowIterator.forEachRemaining(row -> {
				DTYPE dtype = (DTYPE) row.get(discriminatorColumn);
				I id = identifierAssembler.assemble(row);
				result.computeIfAbsent(polymorphismPolicy.getClass(dtype), k -> new KeepOrderSet<>()).add(id);
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
	private static class SingleLoadEntityJoinTree<C, I, DTYPE> extends EntityJoinTree<C, I> {
		
		public <T extends Table<T>> SingleLoadEntityJoinTree(ConfiguredRelationalPersister<C, I> mainPersister,
															 Set<? extends ConfiguredRelationalPersister<C, I>> subPersisters,
															 Column<T, DTYPE> discriminatorColumn,
															 SingleTablePolymorphism<C, DTYPE> polymorphismPolicy) {
			super(self -> new SingleTableRootJoinNode<>(
					self,
					mainPersister,
					subPersisters,
					discriminatorColumn,
					polymorphismPolicy)
			);
		}
	}
}
