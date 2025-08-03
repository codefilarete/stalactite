package org.codefilarete.stalactite.engine.runtime.jointable;

import java.sql.ResultSet;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.codefilarete.stalactite.engine.runtime.AbstractPolymorphicEntityFinder;
import org.codefilarete.stalactite.engine.runtime.ColumnCloneAwareOrderBy;
import org.codefilarete.stalactite.engine.runtime.ConfiguredRelationalPersister;
import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree;
import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree.JoinType;
import org.codefilarete.stalactite.engine.runtime.load.EntityMerger.EntityMergerAdapter;
import org.codefilarete.stalactite.engine.runtime.load.EntityTreeQueryBuilder;
import org.codefilarete.stalactite.engine.runtime.load.EntityTreeQueryBuilder.EntityTreeQuery;
import org.codefilarete.stalactite.engine.runtime.load.JoinTableRootJoinNode;
import org.codefilarete.stalactite.engine.runtime.load.MergeJoinNode;
import org.codefilarete.stalactite.engine.runtime.load.MergeJoinNode.MergeJoinRowConsumer;
import org.codefilarete.stalactite.query.ConfiguredEntityCriteria;
import org.codefilarete.stalactite.query.builder.QuerySQLBuilderFactory.QuerySQLBuilder;
import org.codefilarete.stalactite.query.model.LimitAware;
import org.codefilarete.stalactite.query.model.OrderByChain;
import org.codefilarete.stalactite.query.model.Query;
import org.codefilarete.stalactite.query.model.Selectable;
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
	
	private final T mainTable;
	private final SingleLoadEntityJoinTree<C, I> singleLoadEntityJoinTree;
	
	public JoinTablePolymorphismEntityFinder(
			ConfiguredRelationalPersister<C, I> mainPersister,
			Map<? extends Class<C>, ? extends ConfiguredRelationalPersister<C, I>> persisterPerSubclass,
			ConnectionProvider connectionProvider,
			Dialect dialect) {
		super(mainPersister, persisterPerSubclass, connectionProvider, dialect);
		this.mainTable = (T) mainPersister.getMainTable();
		this.singleLoadEntityJoinTree = buildSingleLoadEntityJoinTree(mainPersister);
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
	public Set<C> selectWithSingleQuery(ConfiguredEntityCriteria where, Consumer<OrderByChain<?>> orderByClauseConsumer, Consumer<LimitAware<?>> limitAwareConsumer) {
		LOGGER.debug("Finding entities in a single query with criteria {}", where);
		if (hasSubPolymorphicPersister) {
			LOGGER.debug("Single query was asked but due to sub-polymorphism the query is made in 2 phases");
			return selectIn2Phases(where, orderByClauseConsumer, limitAwareConsumer);
		} else {
			return super.selectWithSingleQuery(where, orderByClauseConsumer, limitAwareConsumer, singleLoadEntityJoinTree, dialect, connectionProvider);
		}
	}
	
	@Override
	public Set<C> selectIn2Phases(ConfiguredEntityCriteria where, Consumer<OrderByChain<?>> orderByClauseConsumer, Consumer<LimitAware<?>> limitAwareConsumer) {
		LOGGER.debug("Finding entities in 2-phases query with criteria {}", where);
		EntityTreeQuery<C> entityTreeQuery = new EntityTreeQueryBuilder<>(mainEntityJoinTree, dialect.getColumnBinderRegistry()).buildSelectQuery();
		Query query = entityTreeQuery.getQuery();
		persisterPerSubclass.values().forEach(subclassPersister -> {
			query.getFrom().leftOuterJoin(mainTable.getPrimaryKey(), subclassPersister.getMainTable().getPrimaryKey());
			((T) subclassPersister.getMainTable()).getPrimaryKey().getColumns().forEach(column -> {
				query.select(column, column.getAlias());
			});
		});
		
		QuerySQLBuilder sqlQueryBuilder = dialect.getQuerySQLBuilderFactory().queryBuilder(query, where.getCriteria(), entityTreeQuery.getColumnClones());
		
		// selecting ids and their entity type
		Map<Selectable<?>, ResultSetReader<?>> columnReaders = new HashMap<>();
		query.getColumns().forEach((selectable) -> columnReaders.put(selectable, dialect.getColumnBinderRegistry().getBinder((Column) selectable)));
		orderByClauseConsumer.accept(new ColumnCloneAwareOrderBy(query.orderBy(), entityTreeQuery.getColumnClones()));
		limitAwareConsumer.accept(query.orderBy());
		
		Map<Class, Set<I>> idsPerSubtype = readIds(sqlQueryBuilder.toPreparableSQL().toPreparedSQL(new HashMap<>()), columnReaders, query.getAliases());
		
		Set<I> ids = idsPerSubtype.values().stream().flatMap(Collection::stream).collect(Collectors.toSet());
		
		if (hasSubPolymorphicPersister) {
			LOGGER.debug("Asking sub-polymorphic persisters to load the entities");
			Set<C> result = new HashSet<>();
			idsPerSubtype.forEach((k, v) -> result.addAll(persisterPerSubclass.get(k).select(v)));
			return result;
		} else {
			return selectWithSingleQuery(newWhereIdClause(ids),
					orderByChain -> { /* No order by since we are in a Collection criteria, sort we'll be made downstream in memory see EntityCriteriaSupport#wrapGraphload() */},
					limitAware -> { /* No limit since we already have limited our result through the selection of the ids */});
		}

	}
	
	private Map<Class, Set<I>> readIds(PreparedSQL preparedSQL, Map<Selectable<?>, ResultSetReader<?>> columnReaders, Map<Selectable<?>, String> aliases) {
		// Below we keep order of given entities mainly to get steady unit tests. Meanwhile, this may have performance
		// impacts but very difficult to measure
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
