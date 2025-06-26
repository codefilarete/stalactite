package org.codefilarete.stalactite.engine.runtime.singletable;

import java.sql.ResultSet;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.codefilarete.reflection.AccessorChain;
import org.codefilarete.stalactite.engine.PolymorphismPolicy.SingleTablePolymorphism;
import org.codefilarete.stalactite.engine.runtime.AbstractPolymorphicEntityFinder;
import org.codefilarete.stalactite.engine.runtime.AbstractPolymorphismPersister;
import org.codefilarete.stalactite.engine.runtime.ColumnCloneAwareOrderBy;
import org.codefilarete.stalactite.engine.runtime.ConfiguredRelationalPersister;
import org.codefilarete.stalactite.engine.runtime.EntityCriteriaSupport;
import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree;
import org.codefilarete.stalactite.engine.runtime.load.EntityTreeQueryBuilder;
import org.codefilarete.stalactite.engine.runtime.load.EntityTreeQueryBuilder.EntityTreeQuery;
import org.codefilarete.stalactite.engine.runtime.load.JoinTableRootJoinNode;
import org.codefilarete.stalactite.engine.runtime.load.SingleTableRootJoinNode;
import org.codefilarete.stalactite.mapping.AccessorWrapperIdAccessor;
import org.codefilarete.stalactite.mapping.ColumnedRow;
import org.codefilarete.stalactite.mapping.id.assembly.IdentifierAssembler;
import org.codefilarete.stalactite.query.ConfiguredEntityCriteria;
import org.codefilarete.stalactite.query.builder.QuerySQLBuilderFactory.QuerySQLBuilder;
import org.codefilarete.stalactite.query.model.LimitAware;
import org.codefilarete.stalactite.query.model.Operators;
import org.codefilarete.stalactite.query.model.OrderByChain;
import org.codefilarete.stalactite.query.model.Query;
import org.codefilarete.stalactite.query.model.Selectable;
import org.codefilarete.stalactite.sql.ConnectionProvider;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.PrimaryKey;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.RowIterator;
import org.codefilarete.stalactite.sql.statement.PreparedSQL;
import org.codefilarete.stalactite.sql.statement.ReadOperation;
import org.codefilarete.stalactite.sql.statement.SQLExecutionException;
import org.codefilarete.stalactite.sql.statement.binder.ResultSetReader;
import org.codefilarete.tool.VisibleForTesting;
import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.tool.collection.KeepOrderMap;
import org.codefilarete.tool.collection.KeepOrderSet;

import static org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree.ROOT_STRATEGY_NAME;

/**
 * @author Guillaume Mary
 */
public class SingleTablePolymorphismEntityFinder<C, I, T extends Table<T>, DTYPE> extends AbstractPolymorphicEntityFinder<C, I, T> {
	
	@VisibleForTesting
	static final String DISCRIMINATOR_ALIAS = "DISCRIMINATOR";
	
	private final IdentifierAssembler<I, T> identifierAssembler;
	private final ConfiguredRelationalPersister<C, I> mainPersister;
	private final Column<T, DTYPE> discriminatorColumn;
	private final SingleTablePolymorphism<C, DTYPE> polymorphismPolicy;
	private final EntityJoinTree<C, I> singleLoadEntityJoinTree;
	private final boolean hasSubPolymorphicPersister;
	
	public SingleTablePolymorphismEntityFinder(ConfiguredRelationalPersister<C, I> mainPersister,
											   Map<? extends Class<C>, ? extends ConfiguredRelationalPersister<C, I>> persisterPerSubclass,
											   Column<T, DTYPE> discriminatorColumn,
											   SingleTablePolymorphism<C, DTYPE> polymorphismPolicy,
											   ConnectionProvider connectionProvider,
											   Dialect dialect) {
		super(mainPersister.getEntityJoinTree(), persisterPerSubclass, connectionProvider, dialect);
		this.identifierAssembler = mainPersister.getMapping().getIdMapping().getIdentifierAssembler();
		this.mainPersister = mainPersister;
		this.discriminatorColumn = discriminatorColumn;
		this.polymorphismPolicy = polymorphismPolicy;
		this.singleLoadEntityJoinTree = buildSingleLoadEntityJoinTree(mainPersister, persisterPerSubclass);
		this.hasSubPolymorphicPersister = Iterables.find(persisterPerSubclass.values(), subPersister -> subPersister instanceof AbstractPolymorphismPersister) != null;
	}
	
	private SingleLoadEntityJoinTree<C, I, DTYPE> buildSingleLoadEntityJoinTree(ConfiguredRelationalPersister<C, I> mainPersister, Map<? extends Class<C>, ? extends ConfiguredRelationalPersister<C, I>> persisterPerSubclass) {
		SingleLoadEntityJoinTree<C, I, DTYPE> result = new SingleLoadEntityJoinTree<>(
				mainPersister,
				new HashSet<>(persisterPerSubclass.values()),
				discriminatorColumn,
				polymorphismPolicy
		);
		// we project main persister tree to keep its relations
		mainPersister.getEntityJoinTree().projectTo(result, ROOT_STRATEGY_NAME);
		// we add the joins of the sub-persister to the whole graph to make it load its relations
		persisterPerSubclass.values().forEach(subPersister -> {
			subPersister.getEntityJoinTree().projectTo(result, ROOT_STRATEGY_NAME);
		});
		return result;
	}
	
	@Override
	public Set<C> selectWithSingleQuery(ConfiguredEntityCriteria where, Consumer<OrderByChain<?>> orderByClauseConsumer, Consumer<LimitAware<?>> limitAwareConsumer) {
		LOGGER.debug("Finding entities in a single query with criteria {}", where);
		if (hasSubPolymorphicPersister) {
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
		
		QuerySQLBuilder sqlQueryBuilder = dialect.getQuerySQLBuilderFactory().queryBuilder(query, where.getCriteria(), entityTreeQuery.getColumnClones());
		
		// First phase : selecting ids (made by clearing selected elements for performance issue)
		query.getSelectDelegate().clear();
		PrimaryKey<T, I> pk = ((T) mainEntityJoinTree.getRoot().getTable()).getPrimaryKey();
		pk.getColumns().forEach(column -> query.select(column, column.getAlias()));
		query.select(discriminatorColumn, DISCRIMINATOR_ALIAS);
		
		// selecting ids and their entity type
		Map<String, ResultSetReader> columnReaders = new HashMap<>();
		Map<Selectable<?>, String> aliases = query.getAliases();
		aliases.forEach((selectable, s) -> columnReaders.put(s, dialect.getColumnBinderRegistry().getBinder((Column) selectable)));
		ColumnedRow columnedRow = new ColumnedRow(aliases::get);
		orderByClauseConsumer.accept(new ColumnCloneAwareOrderBy(query.orderBy(), entityTreeQuery.getColumnClones()));
		limitAwareConsumer.accept(query.orderBy());
		
		Map<Class, Set<I>> idsPerSubtype = readIds(sqlQueryBuilder.toPreparableSQL().toPreparedSQL(new HashMap<>()), columnReaders, columnedRow);
		
		// Second phase : selecting entities by delegating it to each subclass loader
		// It will generate 1 query per found subclass, made as this :
		// - to avoid superfluous join and complex query in case of relation
		// - make it simpler to implement
		Set<I> ids = idsPerSubtype.values().stream().flatMap(Collection::stream).collect(Collectors.toSet());
		
		if (hasSubPolymorphicPersister) {
			Set<C> result = new HashSet<>();
			idsPerSubtype.forEach((k, v) -> result.addAll(persisterPerSubclass.get(k).select(v)));
			return result;
		} else {
			AccessorWrapperIdAccessor<C, I> idAccessor = (AccessorWrapperIdAccessor<C, I>) mainPersister.getMapping().getIdMapping().getIdAccessor();
			// Because we only need the id in the where clause, we don't need JoinTablePolymorphismPersister's EntityCriteriaSupport that contains all
			// the potential criteria, hence we can instantiate a new one for our local need
			EntityCriteriaSupport<C> whereId = new EntityCriteriaSupport<>(mainPersister.getMapping())
					.and(new AccessorChain<>(idAccessor.getIdAccessor()), Operators.in(ids));
			return selectWithSingleQuery(whereId,
					orderByChain -> { /* No order by since we are in a Collection criteria, sort we'll be made downstream in memory see EntityCriteriaSupport#wrapGraphload() */},
					limitAware -> { /* No limit since we already have limited our result through the selection of the ids */});
		}
	}
	
	private Map<Class, Set<I>> readIds(PreparedSQL preparedSQL, Map<String, ResultSetReader> columnReaders, ColumnedRow columnedRow) {
		try (ReadOperation<Integer> closeableOperation = dialect.getReadOperationFactory().createInstance(preparedSQL, connectionProvider)) {
			ResultSet resultSet = closeableOperation.execute();
			RowIterator rowIterator = new RowIterator(resultSet, columnReaders);
			// Below we keep order of given entities mainly to get steady unit tests. Meanwhile, this may have performance
			// impacts but very difficult to measure
			Map<Class, Set<I>> result = new KeepOrderMap<>();
			rowIterator.forEachRemaining(row -> {
				DTYPE dtype = (DTYPE) row.get(DISCRIMINATOR_ALIAS);
				I id = identifierAssembler.assemble(row, columnedRow);
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
